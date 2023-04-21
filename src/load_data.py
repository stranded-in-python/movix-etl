"""Loading data from Postgresql to ElasticSearch"""
import logging
from time import sleep

from postgres_to_es.config.settings import settings
from postgres_to_es.connections import (
    ElasticConnectionManager,
    PostgresConnectionManager,
    RedisConnectionManager,
)
from postgres_to_es.enrichers import PostgresEnricherManager
from postgres_to_es.exceptions import Error
from postgres_to_es.loaders import ElasticMoviesLoader
from postgres_to_es.mergers import FilmWorkPostgresMerger
from postgres_to_es.producers import PostgresProducer
from postgres_to_es.state import RedisStorage, State
from postgres_to_es.transformers import FilmWork2MoviesTransformer

logger = logging.getLogger()
logger.setLevel(logging.INFO)


class ExtractionManager:
    def __init__(
        self,
        postgres: PostgresConnectionManager,
        redis: RedisConnectionManager,
        elastic: ElasticConnectionManager,
    ):
        self.postgres = postgres
        self.redis = redis
        self.elastic = elastic
        self.storage = RedisStorage(self.redis)
        self.state = State(self.storage)
        self.producer = PostgresProducer(self.state, self.postgres)
        self.enricher = PostgresEnricherManager(self.postgres)
        self.merger = FilmWorkPostgresMerger(self.postgres)
        self.transformer = FilmWork2MoviesTransformer()
        self.loader = ElasticMoviesLoader(elastic)

    logging.info("Executing etl...")

    def execute_etl(self):
        logging.debug("Producing...")
        for table, changed in self.producer.scan():
            logging.info(f"Produced keys for table {table}")
            logging.info(f"Enriching keys for table {table}")
            try:
                for film_keys in self.enricher.enrich(table, changed):
                    logging.info("Merging films")
                    for films in self.merger.merge(film_keys):
                        logging.info("Transforming films to documents...")
                        film_docs = self.transformer.transform(films)
                        logging.info("Sending docs to elastic...")
                        self.loader.load(film_docs)
                else:
                    self.producer.set_state(table)
            except Error as e:
                logging.error(e)
                continue
        logging.info("Finished etl")


def transfer():
    """Основной метод загрузки данных из Postgres в ElasticSearch"""

    logging.debug("Beginning the extraction process...")
    logging.debug("Trying to establsh connection with db...")

    while True:
        try:
            with (
                ElasticConnectionManager() as elastic,
                PostgresConnectionManager() as postgres,
                RedisConnectionManager() as redis,
            ):
                logging.info("Connected")
                manager = ExtractionManager(postgres, redis, elastic)
                manager.execute_etl()
        except Exception as e:
            logging.error(type(e))
            logging.error(e)
            logging.error("Exiting...")
            return

        logging.info("Sleeping...")
        sleep(settings.wait_up_to)


if __name__ == "__main__":
    transfer()
