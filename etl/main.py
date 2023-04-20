import datetime
import json
import time

from extractors import get_filmworks_by_id, get_slice_film_work_ids_by_table

from etl.settings.state import Fields as StateFields
from etl.settings.redis import Settings as RedisSettings
from etl.settings.postgres import Settings as PostgresSettings
from etl.settings.elasticsearch import ClientSettings as ES_Settings
from etl.settings.logger import logger

from etl.clients.elasticsearch import ElasticSearchClient
from etl.clients.storage import State
from etl.clients.redis import create_redis_adapter, RedisStorage
from etl.clients.postgres import PostgresSQLClient


def run_etl():
    logger.info('Starting ETL...')

    postgres_settings = PostgresSettings()
    redis_settings = RedisSettings()
    es_settings = ES_Settings()
    state_fields = StateFields()

    state = State(
        RedisStorage(
            adapter=create_redis_adapter(redis_settings, logger)
        ),
        state_fields
    )
    es_client = ElasticSearchClient(
        es_settings,
        logger=logger,
    )

    tables = list(v for k, v in postgres_settings.tables)

    if not es_client.index_exists():
        with open(
                file=es_client.settings.get('scheme_path'),
                mode='r',
                encoding='utf-8'
        ) as file:
            es_schema = json.load(fp=file)
            es_client.es.indices.create(
                index='movies',
                settings=es_schema.get('settings'),
                mappings=es_schema.get('mappings'),
            )

    while True:
        postgres_client = PostgresSQLClient(postgres_settings, logger)

        start_pivot = state.get('start_pivot')
        current_table = state.get('current_table')

        if current_table is None:
            current_table = tables[0]
            state.set('current_table', current_table)

        logger.warning(f'Start pivot {start_pivot}')

        film_id_modified_columns = get_slice_film_work_ids_by_table(
                client=postgres_client,
                table_name=current_table,
                by_date=start_pivot,
                slice_limit=100,
        )

        if len(film_id_modified_columns.rows) <= 1:
            logger.info(
                "{table} data has been updated".format(table=current_table)
            )
            next_index = (tables.index(current_table) + 1) \
                % (len(tables))
            state.set(
                'current_table',
                tables[next_index]
            )
            logger.info("ETL is idling..")
            time.sleep(10)
            continue

        state.set(
            'start_pivot',
            datetime.datetime.strftime(
                film_id_modified_columns.rows[0][1],
                redis_settings.get('dataformat'),
            )
        )
        state.set(
            'end_pivot',
            datetime.datetime.strftime(
                film_id_modified_columns.rows[-1][1],
                redis_settings.get('dataformat'),
            )
        )

        id_column_index = film_id_modified_columns \
            .headers \
            .index('id')
        es_client.bulk_index_documents(
            get_filmworks_by_id(
                client=postgres_client,
                ids=tuple(
                    row[id_column_index]
                    for row in film_id_modified_columns.rows
                )
            )
        )
        state.set(
            'start_pivot',
            state.get('end_pivot')
        )
        state.set('end_pivot', None)

        state.save()


if __name__ == '__main__':
    run_etl()
