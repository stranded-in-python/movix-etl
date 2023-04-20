from datetime import datetime
from typing import Optional, Union, Tuple, List

from etl.clients.postgres import PostgresSQLClientBase, QueryResult
from etl.models import FilmWork, Person


def slice_table_by_id(
    client: PostgresSQLClientBase,
    table_name: str,
    by_date: Optional[datetime] = None,
    slice_limit: int = 100,
) -> QueryResult:

    where = ""
    if by_date is not None:
        if where == "":
            where = "WHERE "
        where = f""" {where} modified::timestamptz >= TO_TIMESTAMP('{by_date}', 'YYYY-MM-DD HH24:MI:SS.US') """

    query_text = '''
        SELECT
            "id"::varchar,
            "modified"
        FROM
            {table_name} 
        {where}
        ORDER BY "modified" ASC 
        LIMIT {limit}'''\
        .format(
            table_name=table_name,
            where=where,
            limit=slice_limit,
        )

    return client.execute(query_text)


def get_filmworks_by_id(
        client: PostgresSQLClientBase,
        ids: Tuple[str, ...]
) -> Tuple[FilmWork]:

    data = []
    query_text = '''
        SELECT
            fw.id::varchar as id,
            fw.rating as imdb_rating,
            COALESCE(json_agg(DISTINCT g.name), '[]') as genre,
            fw.title as title,
            fw.description as description,
            COALESCE (
                json_agg(DISTINCT p.full_name) FILTER (
                    WHERE
                        p.id is not null
                        AND pfw.profession::varchar = 'director'

                ),
                '[]'
            ) as director,
            COALESCE (
                json_agg(
                    DISTINCT jsonb_build_object(
                        'id', p.id,
                        'name', p.full_name
                    )
                ) FILTER (WHERE
                 p.id is not null
                 AND pfw.profession::varchar = 'actor'),
                '[]'
            ) as actors,
            COALESCE (
                json_agg(DISTINCT p.full_name) FILTER (
                    WHERE
                        p.id is not null
                        AND pfw.profession::varchar = 'actor'

                ),
                '[]'
            ) as actors_names,
            COALESCE (
                json_agg(
                    DISTINCT jsonb_build_object(
                        'id', p.id,
                        'name', p.full_name
                    )
                ) FILTER (WHERE
                 p.id is not null
                 AND pfw.profession::varchar = 'writer'),
                '[]'
            ) as writers,
            COALESCE (
                json_agg(DISTINCT p.full_name) FILTER (
                    WHERE
                        p.id is not null
                        AND pfw.profession::varchar = 'writer'

                ),
                '[]'
            ) as writers_names
        FROM content.film_work fw
        LEFT JOIN content.person_film_work pfw 
        ON pfw.film_work_id = fw.id
        LEFT JOIN content.person p 
        ON p.id = pfw.person_id
        LEFT JOIN content.genre_film_work gfw 
        ON gfw.film_work_id = fw.id
        LEFT JOIN content.genre g 
        ON g.id = gfw.genre_id
        WHERE fw.id::varchar IN ({ids})
        GROUP BY fw.id
        LIMIT 100'''.format(ids=', '.join(f"'{_}'" for _ in ids))

    query_result = client.execute(
        query_text
    )

    data = tuple(
        serialiaze_filmwork(
            **dict(zip(query_result.headers, row))
        ) for row in query_result.rows
    )
    return data


def get_slice_film_work_ids_by_table(
        client: PostgresSQLClientBase,
        table_name: str,
        slice_limit: Union[int, str], 
        by_date: Optional[datetime] = None,
) -> QueryResult:

    query_text = ""
    if by_date is None:
        by_date = datetime(1, 1, 1, 1, 1, 1, 1,)
    where_condition = f"""
        modified::timestamptz >= TO_TIMESTAMP('{by_date}', 'YYYY-MM-DD HH24:MI:SS.US') 
    """

    if table_name == client.tables.get('film_work_table_name'):
        query_text = '''
            SELECT
                "id"::varchar,
                "modified"
            FROM
                {table_name} 
            WHERE {where}
            ORDER BY
                "modified" ASC, 
                "id" ASC
            LIMIT {limit}
        '''.format(table_name=table_name,
                   where=where_condition,
                   limit=slice_limit,
                   )

    elif table_name == client.tables.get('person_table_name'):
        query_text = '''
            SELECT DISTINCT 
                fw.id::varchar,
                fw.modified
            FROM content.film_work fw
            LEFT JOIN content.person_film_work pfw
                ON fw.id = pfw.film_work_id
            WHERE 
                pfw.person_id IN (
                    SELECT id
                    FROM content.person
                    WHERE {where}
                    ORDER BY modified ASC
                    LIMIT {limit}
                )
                AND {where}
            ORDER BY fw.modified ASC
            LIMIT {limit}
        '''.format(table_name=table_name,
                   where=where_condition,
                   limit=slice_limit,
                   )

    elif table_name == client.tables.get('genre_table_name'):
        query_text = '''
            SELECT DISTINCT 
                fw.id::varchar,
                fw.modified
            FROM content.film_work fw
                LEFT JOIN content.genre_film_work gfw
                ON fw.id = gfw.film_work_id
            WHERE 
                gfw.genre_id IN (
                    SELECT id
                    FROM content.genre
                    WHERE {where}
                    ORDER BY modified ASC
                )
                AND {where}
            ORDER BY fw.modified ASC
            LIMIT {limit}
        '''.format(table_name=table_name,
                   where=where_condition,
                   limit=slice_limit,
                   )

    return client.execute(query_text)


def serialiaze_filmwork(**kwargs):
    if 'actors' in kwargs:
        kwargs['actors'] = tuple(dict(Person(**_kwargs)) for _kwargs in kwargs['actors'])
    if 'writers' in kwargs:
        kwargs['writers'] = tuple(dict(Person(**_kwargs)) for _kwargs in kwargs['writers'])

    for k, v in kwargs.items():
        if v is list:
            kwargs[k] = tuple(kwargs[k])

    return FilmWork(**kwargs)
