from typing import Iterable

from ..models import Entry
from .abc import EnricherManager, PostgresEnricher


class FilmWorkEnricher(PostgresEnricher):
    table_name = "content.film_work"

    sql = (
        "SELECT fw.modified, pfw.person_id "
        "FROM content.film_work fw JOIN content.person_film_work pfw ON "
        "fw.id = pfw.film_work_id WHERE "
        "fw.id IN %s ORDER BY fw.modified ASC;"
    )


class PersonEnricher(PostgresEnricher):
    table_name = "content.person"

    def enrich(self, entries: Iterable[Entry]) -> Iterable[Iterable[Entry]]:
        yield entries


class GenreEnricher(PostgresEnricher):
    table_name = "content.genre"

    sql = (
        "SELECT g.modified, pfw.person_id "
        "FROM content.genre AS g join content.genre_film_work AS gfw ON "
        "g.id = gfw.genre_id JOIN content.person_film_work AS pfw ON "
        "gfw.film_work_id = pfw.film_work_id WHERE g.id IN %s ORDER BY g.modified ASC;"
    )


class PersonFilmWorkEnricher(PostgresEnricher):
    table_name = "content.person_film_work"

    sql = (
        "SELECT created, person_id FROM content.person_film_work "
        "WHERE id IN %s ORDER BY created ASC;"
    )


class GenreFilmWorkEnricher(PostgresEnricher):
    table_name = "content.genre_film_work"

    sql = (
        "SELECT gfw.created, pfw.person_id FROM content.genre_film_work AS gfw"
        "JOIN content.person_film_work AS pfw ON gfw.film_work_id = pfw.film_work_id"
        "WHERE gfw.id IN %s ORDER BY gfw.created ASC;"
    )


class PersonEnricherManager(EnricherManager):
    enrichers = (PersonEnricher,)
