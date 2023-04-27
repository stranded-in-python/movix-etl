import logging
from abc import ABC, abstractmethod
from typing import Iterable

from ..connections import ConnectionManager, PostgresConnectionManager
from ..exceptions import DataInconsistentError
from ..models import Entry


class Enricher(ABC):
    table_name = ""

    def __init__(self, manager: ConnectionManager):
        self.manager = manager

    @abstractmethod
    def enrich(
        self, entries: Iterable[Entry], pack_size: int = 1000
    ) -> Iterable[Iterable[Entry]]:
        ...


def enrich(
    manager: PostgresConnectionManager, sql, pack_size, vals
) -> Iterable[Iterable[Entry]]:
    for rows in manager.fetchmany(sql, pack_size, sql_vars=(vals,), itersize=5000):
        if not rows:
            break

        rows = (Entry(modified=row[0], id=row[1]) for row in rows)

        yield rows


class PostgresEnricher(Enricher, ABC):
    sql = ""

    def __init__(self, manager: PostgresConnectionManager):
        self.manager = manager
        if "." not in self.table_name:
            raise DataInconsistentError(
                f"Schema is not set for table {self.table_name}"
            )

    @property
    def short_tabname(self):
        return self.table_name.split(".")[1]

    def enrich(
        self, entries: Iterable[Entry], pack_size: int = 1000
    ) -> Iterable[Iterable[Entry]]:
        if not self.sql:
            return ()

        vals = tuple(str(entry.id) for entry in entries)
        return enrich(self.manager, self.sql, pack_size, vals)


class EnricherManager(ABC):
    enrichers: tuple[type[Enricher], ...]

    def __init__(self, manager: ConnectionManager):
        self.manager = manager

        self._enrichers = {
            enricher.table_name: enricher(manager) for enricher in self.enrichers
        }

    def enrich(
        self, table_name: str, entries: Iterable[Entry]
    ) -> Iterable[Iterable[Entry]]:
        enricher = self._enrichers.get(table_name)

        if not enricher:
            logging.warning(f"No enricher created for table {table_name}!")
            return []

        return enricher.enrich(entries)
