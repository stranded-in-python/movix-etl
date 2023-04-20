import os
from datetime import datetime
from time import sleep

import logging
from components.logger import logger
from dotenv import load_dotenv

from components.psql_extractor import PSQLExtractor, PSQLTransformer
from components.elastic_loader import ElasticSaver
from components.state_status import JsonFileStorage, State


logger()

load_dotenv()

TIME_SLEEP = float(os.environ.get('MAIN_TIME_SLEEP'))

TABLES= ['person', 'genre', 'film_work']

if __name__ == '__main__':
    extractor = PSQLExtractor()
    state = State(JsonFileStorage('state.json'))
    es_saver = ElasticSaver()

    while True:
        logging.info('Starting...')
        curr_date = state.get_state('date')
        curr_offset = state.get_state('offset')
        sleep(TIME_SLEEP)
        for curr_table in TABLES:
            data_generator = extractor.get_ids(curr_table, curr_date, curr_offset)
            ids = []
            for lst in data_generator:
                for row in lst:
                    ids.append(row['id'])
            if not ids:
                state.set_state('date', datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f%z'))
                state.set_state('offset', 0)
                continue

            fw_ids = extractor.get_fw_id(ids, curr_table)

            film_data = extractor.get_all_parsed_data(fw_ids)

            extractor.batch_size += curr_offset
            state.set_state('offset', curr_offset)
            
            es_film_works = PSQLTransformer.parse_from_postgres_to_es(film_data)

            rows_count, errors = es_saver.send_data_to_es(es_saver.es, es_film_works)
            
            state.set_state('offset', 0)
            state.set_state('date', datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f%z'))
