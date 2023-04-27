from typing import Iterable

from ..models import Entry
from .abc import EnricherManager, PostgresEnricher


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
        "select genre.modified, genre_film_work.film_work_id "
        "from content.genre join content.genre_film_work on "
        "genre.id = genre_film_work.genre_id where "
        "genre.id in %s;"
    )


class PersonFilmWorkEnricher(PostgresEnricher):
    table_name = "content.person_film_work"

    sql = "select created, film_work_id from content.person_film_work where id in %s;"


class GenreFilmWorkEnricher(PostgresEnricher):
    table_name = "content.genre_film_work"

    sql = "select created, film_work_id from content.genre_film_work where id in %s;"


class FilmsEnricherManager(EnricherManager):
    enrichers = (
        FilmWorkEnricher,
        PersonEnricher,
        GenreEnricher,
        PersonFilmWorkEnricher,
        GenreFilmWorkEnricher,
    )
