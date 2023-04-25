from abc import ABC, abstractmethod
from typing import Iterable

from psycopg2.extras import DictRow

from .config.settings import settings
from .connections import ConnectionManager, PostgresConnectionManager
from .models import Entry, FilmWork


class Merger(ABC):
    def __init__(self, manager: ConnectionManager):
        self.manager = manager

    @abstractmethod
    def merge(self, entries: Iterable[Entry]) -> Iterable[Iterable[FilmWork]]:
        ...


class PostgresMerger(Merger, ABC):
    def __init__(self, manager: PostgresConnectionManager):
        self.manager = manager

    @abstractmethod
    def transform_to_entries(self, rows: Iterable[DictRow]) -> Iterable[FilmWork]:
        ...


class FilmWorkPostgresMerger(PostgresMerger):
    def transform_to_entries(self, rows: Iterable[DictRow]) -> Iterable[FilmWork]:
        entries = (
            FilmWork.parse_obj(
                {
                    field.lower(): value
                    for field, value in zip(
                        FilmWork.schema()["properties"].keys(), row  # type: ignore
                    )
                }
            )
            for row in rows
        )
        return entries

    def merge(self, entries: Iterable[Entry]) -> Iterable[Iterable[FilmWork]]:
        sql = (
            "SELECT fw.id, fw.title, fw.description, fw.rating,"
            "array_agg(DISTINCT g.name) as genre, "
            """COALESCE (
               json_agg(
                   DISTINCT jsonb_build_object(
                       'id', p.id,
                       'name', p.full_name
                   )
               ) FILTER (WHERE p.id is not null and pfw.role = 'actor'),
               '[]'
           ) as actors, """
            """COALESCE (
               json_agg(
                   DISTINCT jsonb_build_object(
                       'id', p.id,
                       'name', p.full_name
                   )
               ) FILTER (WHERE p.id is not null and pfw.role = 'director'),
               '[]'
           ) as directors, """
            """COALESCE (
               json_agg(
                   DISTINCT jsonb_build_object(
                       'id', p.id,
                       'name', p.full_name
                   )
               ) FILTER (WHERE p.id is not null and pfw.role = 'writer'),
               '[]'
           ) as writers ,"""
            """COALESCE (
                json_agg(
                    DISTINCT jsonb_build_object('id', g.id, 'name', g.name)
                ) FILTER (WHERE g.id is not null and gfw.genre_id = g.id),
                '[]'
            ) as genres """
            "FROM content.film_work fw "
            "LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id "
            "LEFT JOIN content.person p ON p.id = pfw.person_id "
            "LEFT JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id "
            "LEFT JOIN content.genre g ON g.id = gfw.genre_id "
            "WHERE fw.id in %s GROUP BY fw.id ORDER BY fw.modified ASC ;"
        )

        vals = tuple(str(entry.id) for entry in entries)

        if not vals:
            return ()

        for rows in self.manager.fetchmany(
            sql, settings.elastic_pack_size, sql_vars=(vals,)
        ):
            if not rows:
                break

            rows = self.transform_to_entries(rows)

            yield rows
