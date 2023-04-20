from abc import ABC, abstractmethod
from typing import Iterable

from .connections import ConnectionManager, PostgresConnectionManager
from .exceptions import DataInconsistentError
from .models import Entry


class Enricher(ABC):
    table_name = ""

    def __init__(self, manager: ConnectionManager):
        self.manager = manager

    @abstractmethod
    def enrich(
        self, entries: Iterable[Entry], pack_size: int = 1000
    ) -> Iterable[Iterable[Entry]]:
        ...


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

        for rows in self.manager.fetchmany(
            self.sql, pack_size, sql_vars=(vals,), itersize=5000
        ):
            if not rows:
                break

            rows = (Entry(modified=row[0], id=row[1]) for row in rows)

            yield rows


class FilmWorkEnricher(PostgresEnricher):
    table_name = "content.film_work"

    def enrich(self, entries: Iterable[Entry]) -> Iterable[Iterable[Entry]]:
        yield entries


class PersonEnricher(PostgresEnricher):
    table_name = "content.person"

    sql = (
        "select person.modified, person_film_work.film_work_id "
        "from content.person join content.person_film_work on "
        "person.id = person_film_work.person_id where "
        "person.id in %s;"
    )


class GenreEnricher(PostgresEnricher):
    table_name = "content.genre"

    sql = (
        "select genre.modified, film_work_genre.film_work_id "
        "from content.genre join content.film_work_genre on "
        "genre.id = film_work_genre.genre_id where "
        "genre.id in %s;"
    )


class PersonFilmWorkEnricher(PostgresEnricher):
    table_name = "content.person_film_work"

    sql = "select created, film_work_id from content.person_film_work where id in %s;"


class FilmWorkGenreEnricher(PostgresEnricher):
    table_name = "content.film_work_genre"

    sql = "select created, film_work_id from content.film_work_genre where id in %s;"


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
            raise DataInconsistentError(f"No enricher created for table {table_name}!")

        return enricher.enrich(entries)


class PostgresEnricherManager(EnricherManager):
    enrichers = (
        FilmWorkEnricher,
        PersonEnricher,
        GenreEnricher,
        PersonFilmWorkEnricher,
        FilmWorkGenreEnricher,
    )
