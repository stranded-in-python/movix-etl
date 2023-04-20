import logging

from abc import ABC, abstractmethod
from typing import List, Mapping, Tuple

import psycopg
from psycopg.rows import TupleRow
from psycopg.abc import Query
from pydantic import BaseModel

from etl.settings.postgres import Settings
from etl.utils.backoff import backoff


class QueryResult(BaseModel):
    headers: Tuple[str, ...]
    rows: List[TupleRow]


class PostgresSQLClientBase(ABC):
    connection_settings: Mapping
    tables: Mapping

    @abstractmethod
    def execute(self, query: Query, *args, **kwargs) -> QueryResult:
        """Выполнить запрос и вернуть результат запроса с именами колонок"""
        pass


class PostgresSQLClient(PostgresSQLClientBase):
    def __init__(
            self,
            settings: Settings,
            logger: logging.Logger
    ):
        self._connection = {
            'dbname':   settings.get('name'),
            'user':     settings.get('user'),
            'password': settings.get('password'),
            'host':     settings.get('host'),
            'port':     settings.get('port'),
        }
        self.tables = {
            'film_work_table_name':         settings.tables.get('filmwork'),
            'person_table_name':            settings.tables.get('person'),
            'genre_table_name':             settings.tables.get('genre'),
        }
        self.log = logger

    @backoff
    def execute(self, query: Query, *args, **kwargs) -> QueryResult:
        self.log.info('Creating connections with PostgreSQL database..')

        with psycopg.connect(**self._connection) as conn:
            self.log.info('The connetion is established PostgreSQL database.')
            self.log.info('Creating a cursor..')

            with conn.cursor() as cur:
                cur.execute(query, *args, **kwargs)
                self.log.info('The result of the request was received')

                return QueryResult(
                    headers=tuple(x.name for x in cur.description),
                    rows=cur.fetchall()
                )
