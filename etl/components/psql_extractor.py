import os
from dotenv import load_dotenv
from typing import List, Generator
from abc import ABC

import logging
import psycopg2
from psycopg2.extensions import connection as connection
from psycopg2.extras import DictCursor, RealDictRow, DictRow

from .backoff import backoff
from components.models import PSQLFilmwork, ESFilmwork


load_dotenv()

dsl = {'dbname': os.environ.get('DB_NAME'), 'user': os.environ.get('DB_USER'),
    'password': os.environ.get('DB_PASSWORD'), 'host': '127.0.0.1', 'port': 5432}

BATCH_SIZE = 100

class PGConnectorBase:
    
    def __init__(self, logging=logging):
        self.db = None
        self.cursor = None
        self._logging = logging
        self.batch_size = BATCH_SIZE

    @backoff(logging=logging)
    def connect_pg(self) -> None:
        self.db = psycopg2.connect(**dsl, cursor_factory=DictCursor)
        self.cursor = self.db.cursor()

    @backoff(logging=logging)
    def query(self, sql: str) -> List[RealDictRow]:
        try:
            self.cursor.execute(sql)
        except psycopg2.OperationalError:
            self._logging.error('Could not connect to psql')
            self.connect_pg()
            self.cursor.execute(sql)
        result = self.cursor.fetchall()
        return result

    def __del__(self) -> None:
        if self.db:
            self.db.close()



class PSQLExtractor(PGConnectorBase):

    def get_ids(self, table_name: str, date_start: str, offset:str) -> Generator:
        self._logging.info('Collecting %s ids...', table_name)
        self.connect_pg()
        while True:
            sql_tmp = ("select id, updated_at "
                       f"from content.{table_name} "
                       "where updated_at >= %(date)s "
                       "ORDER BY updated_at limit %(limit)s "
                       "offset %(offset)s")
            sql = self.cursor.mogrify(sql_tmp, {
                'date': date_start,
                'limit': self.batch_size,
                'offset': offset
            })
            ids = self.query(sql)
            if not ids:
                self.cursor.close()
                break
            self.cursor.close()
            yield ids

            offset += self.batch_size
            if len(ids) != self.batch_size:
                break

    def get_fw_id(self, table_id: List, table: str):
            self.connect_pg()
            if table == "film_work":
                self.cursor.close()
                return tuple(table_id)
            elif table == "person":
                sql_tmp = (
                "select pfw.film_work_id "
                "from content.person p "
                "left join content.person_film_work pfw on p.id = pfw.person_id "
                "WHERE p.id IN %(ids)s"
            )
                sql = self.cursor.mogrify(sql_tmp, {
                    'ids': tuple(table_id)
                })
            elif table == "genre":
                sql_tmp = (
                "SELECT fw.id FROM film_work fw "
                "LEFT JOIN genre_film_work gfw ON gfw.film_work_id = fw.id "
                "WHERE gfw.genre_id IN %(ids)s"
            )
                sql = self.cursor.mogrify(sql_tmp, {
                    'ids': tuple(table_id)
                })
            fw_ids = self.query(sql)
            self.cursor.close()
            return tuple([item[0] for item in fw_ids]) if fw_ids else tuple()

    def get_all_data(self, fw_ids):
        self.connect_pg()
        sql_tmp = ("SELECT fw.id as fw_id, fw.title, fw.description, "
                   "fw.rating, fw.type, fw.created_at, fw.updated_at, "
                   "pfw.role, p.id as person_id, p.full_name, g.name , g.id as genre_id "
                   "FROM content.film_work fw "
                   "LEFT JOIN content.person_film_work pfw "
                   "ON pfw.film_work_id = fw.id "
                   "LEFT JOIN content.person p "
                   "ON p.id = pfw.person_id "
                   "LEFT JOIN content.genre_film_work gfw "
                   "ON gfw.film_work_id = fw.id "
                   "LEFT JOIN content.genre g ON g.id = gfw.genre_id "
                   "WHERE fw.id IN %(films_id)s")
        sql = self.cursor.mogrify(sql_tmp, {'films_id': fw_ids})
        result = self.query(sql)
        self.cursor.close()
        return result

    def get_all_parsed_data(self, fw_ids):
        self.connect_pg()
        sql_tmp = """
                    SELECT
                    fw.id as fw_id,
                    fw.title,
                    fw.description,
                    fw.rating,
                    fw.type,
                    array_agg(DISTINCT g.name) as genres,
                        COALESCE (
                            json_agg(
                                DISTINCT jsonb_build_object(
                                    'person_name', p.full_name
                                )
                            ) FILTER (WHERE p.id is not null AND pfw.role='director'),
                            '[]'
                        ) as director,
                    COALESCE (
                        json_agg(
                            DISTINCT jsonb_build_object(
                                'person_name', p.full_name
                            )
                        ) FILTER (WHERE p.id is not null AND pfw.role='actor'),
                        '[]'
                    ) as actors_names,
                    COALESCE (
                        json_agg(
                            DISTINCT jsonb_build_object(
                                'person_name', p.full_name
                            )
                        ) FILTER (WHERE p.id is not null AND pfw.role='writer'),
                        '[]'
                    ) as writers_names,
                    COALESCE (
                        json_agg(
                            DISTINCT jsonb_build_object(
                                'id', p.id,
                                'name', p.full_name
                            )
                        ) FILTER (WHERE p.id is not null AND pfw.role='actor'),
                        '[]'
                    ) as actors,
                    COALESCE (
                        json_agg(
                            DISTINCT jsonb_build_object(
                                'id', p.id,
                                'name', p.full_name
                            )
                        ) FILTER (WHERE p.id is not null AND pfw.role='writer'),
                        '[]'
                    ) as writers                   
                    FROM content.film_work fw
                    LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
                    LEFT JOIN content.person p ON p.id = pfw.person_id
                    LEFT JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id
                    LEFT JOIN content.genre g ON g.id = gfw.genre_id
                    WHERE fw.id IN %(films_id)s
                    GROUP BY fw.id
                    ORDER BY fw.updated_at;
        """
        sql = self.cursor.mogrify(sql_tmp, {'films_id': fw_ids})
        result = self.query(sql)
        self.cursor.close()
        return result

class PSQLTransformer(ABC):
    
    def parse_from_postgres_to_es(film_data: List[DictRow]) -> ESFilmwork:
        es_data = []
        for film in film_data:
            es_data.append(ESFilmwork(
                id=film[0], title=film[1], description=film[2], 
                imdb_rating=film[3], genre=film[5],
                director=[dict['person_name'] for dict in film[6]],
                actors_names=[dict['person_name'] for dict in film[7]], 
                writers_names=[dict['person_name'] for dict in film[8]],
                actors=[{'id': person['id'], 'name': person['name']} for person in film[9]],
                writers=[{'id': person['id'], 'name': person['name']} for person in film[10]])
                )
        return es_data
    