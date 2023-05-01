from abc import ABC, abstractmethod
from typing import Iterable

from .connections import ConnectionManager, PostgresConnectionManager
from .models import Entry
from .state import State


def scan(tables, scanning_method) -> Iterable[tuple[str, Iterable]]:
    for table_name, items in tables:
        for rows in scanning_method(table_name, items):
            yield table_name, rows
    return


class Producer(ABC):
    def __init__(self, state: State, manager: ConnectionManager):
        self.state: State = state
        self.not_processed_entities = {}
        self.manager: ConnectionManager = manager

    def set_state(self, table: str, index_name: str):
        entity = self.not_processed_entities[table]
        self.state.set_state(f"{index_name}:{table}", entity)
        # this batch processed sucessfully
        self.not_processed_entities[table] = None

    @abstractmethod
    def scan_table(self, table: str, items: int = 50) -> Iterable:
        ...

    def scan(
        self, tables: Iterable[tuple[str, int]] | None = None
    ) -> Iterable[tuple[str, Iterable]]:
        return scan(tables, self.scan_table)


class PostgresProducer(Producer):
    def __init__(
        self, state: State, manager: PostgresConnectionManager, index_name: str
    ):
        super().__init__(state, manager)
        self.manager: PostgresConnectionManager = manager
        self.index_name = index_name

    def scan_table(self, table: str, pack_size: int) -> Iterable:
        state = self.state.get_state(f"{self.index_name}:{table}")
        date_field = "modified"
        if table in ("content.genre_film_work", "content.person_film_work"):
            date_field = "created"

        sql = (
            f"select {date_field}, id from {table} where {date_field} >= '{state.modified}' "
            f"and id >= '{state.id}' order by {date_field} asc, id asc;"
        )

        for rows in self.manager.fetchmany(sql, pack_size, itersize=5000):
            rows = [Entry(modified=row[0], id=row[1]) for row in rows]

            # Processing didn't go on happy path

            if self.not_processed_entities.get(table):
                return

            # Remembering current batch

            self.not_processed_entities[table] = rows[-1]

            yield rows
