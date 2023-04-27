from typing import Iterable

from ..models import Entry
from .abc import EnricherManager, PostgresEnricher


class FilmWorkEnricher(PostgresEnricher):
    table_name = "content.film_work"

    sql = (
        "SELECT fw.modified, gfw.genre_id FROM content.film_work AS fw "
        "JOIN content.genre_film_work AS gfw ON "
        "fw.film_work_id = gfw.film_work_id WHERE fw.id IN %s "
        "ORDER BY fw.modified ASC;"
    )


class PersonEnricher(PostgresEnricher):
    table_name = "content.person"

    sql = (
        "SELECT p.modified, gfw.genre_id FROM content.person AS p JOIN "
        "content.person_film_work AS pfw ON p.id = pfw.person_id JOIN "
        "content.genre_film_work AS gfw ON pfw.film_work_id = gfw.film_work_id"
        "WHERE p.id IN %s ORDER BY p.modified ASC;"
    )


class GenreEnricher(PostgresEnricher):
    table_name = "content.genre"

    def enrich(self, entries: Iterable[Entry]) -> Iterable[Iterable[Entry]]:
        yield entries


class PersonFilmWorkEnricher(PostgresEnricher):
    table_name = "content.person_film_work"

    sql = (
        "SELECT pfw.created, gfw.genre_id FROM content.person_film_work AS pfw JOIN "
        "content.genre_film_work AS gfw ON pfw.film_work_id = gfw.film_work_id WHERE "
        "pfw.id IN %s ORDER BY pfw.created ASC;"
    )


class GenreFilmWorkEnricher(PostgresEnricher):
    table_name = "content.genre_film_work"

    sql = "SELECT created, genre_id FROM content.genre_film_work WHERE id IN %s;"


class GenreEnricherManager(EnricherManager):
    enrichers = (GenreEnricher,)
