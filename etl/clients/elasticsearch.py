import json
import logging
from typing import Sequence

from elasticsearch import Elasticsearch

from etl.models import FilmWork
from etl.settings.elasticsearch import ClientSettings, IndexSettings

from etl.utils.backoff import backoff


class ElasticSearchClient:
    def __init__(self, settings: ClientSettings, logger: logging.Logger):
        self.es = Elasticsearch(
            [{"host": settings.get('host'),
              "port": settings.get("port"),
              "scheme": settings.get("scheme"),
              }
             ]
        )
        self.settings = settings
        self.logger = logger

    @backoff
    def create_index(self, index_name: str, settings: IndexSettings = None):
        if self.es.indices.exists(index=index_name):
            self.logger.warning(f"Index '{index_name}' already exists.")
            return False

        response = self.es.indices.create(index=index_name, body=settings)
        if response.get("acknowledged", False):
            self.logger.info(f"Index '{index_name}' created successfully.")
            return True

        self.logger.error(f"Error creating index '{index_name}': {response}")
        return False

    @backoff
    def index_exists(self):
        return self.es.indices.exists(index=self.settings.get('scheme'))


    @backoff
    def bulk_index_documents(self, documents: Sequence[FilmWork]):
        _index = self.settings.get('scheme')
        actions = []
        for doc in documents:
            actions.append({
                "index": {
                    "_id": doc.id,
                    "_index": _index,
                    },
                }
            )
            actions.append(
                json.dumps(dict(doc))
            )

        response = self.es.bulk(body=actions)

        if response.body['errors']:
            self.logger.warning(
                f"Bulk indexing completed with \
                {len(response.raw['items'])} failures.")
        else:
            self.logger.info("All documents indexed successfully.")

        return response
