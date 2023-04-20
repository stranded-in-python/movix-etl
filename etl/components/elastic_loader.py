import os
from typing import List, Tuple

import logging
from elasticsearch import Elasticsearch, helpers
from dotenv import load_dotenv

from .backoff import backoff
from .models import ESFilmwork

load_dotenv()

class ElasticSaver:
    def __init__(self):
        self.es_host = os.environ.get('ES_HOST')
        self.es_user = os.environ.get('ES_USER')
        self.es_password = os.environ.get('ES_PASSWORD')
        self.es = Elasticsearch(self.es_host, basic_auth=(self.es_user, self.es_password), verify_certs=False)

    @staticmethod
    @backoff()
    def send_data_to_es(es: Elasticsearch, es_data: List[ESFilmwork]) -> Tuple[int, list]:
        query = [{'_index': 'movies', '_id': data.id, '_source': data.dict()} for data in es_data]
        try:
            rows_count, errors = helpers.bulk(es, query)
        except helpers.BulkIndexError as error:
            logging.exception("BulkIndexError occured, errors: %s", error.errors)
        return rows_count, errors
    
