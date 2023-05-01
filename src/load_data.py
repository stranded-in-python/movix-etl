"""Loading data from Postgresql to ElasticSearch"""
import logging
import os
from time import sleep

from postgres_to_es.config.settings import settings
from postgres_to_es.connections import (
    ElasticConnectionManager,
    ElasticConnector,
    PostgresConnectionManager,
    PostgresConnector,
    RedisConnectionManager,
    RedisConnector,
)
from postgres_to_es.enrichers import (
    EnricherManager,
    FilmsEnricherManager,
    GenreEnricherManager,
    PersonEnricherManager,
)
from postgres_to_es.exceptions import Error
from postgres_to_es.loaders import ElasticLoader
from postgres_to_es.mergers import (
    FilmWorkPostgresMerger,
    GenrePostgresMerger,
    Merger,
    PersonPostgresMerger,
)
from postgres_to_es.producers import PostgresProducer, Producer
from postgres_to_es.state import RedisStorage, State
from postgres_to_es.transformers import (
    FilmWork2MoviesTransformer,
    GenreTransformer,
    PersonTransformer,
    Transformer,
)

logger = logging.getLogger()
logger.setLevel(os.environ.get("ETL_LOG_LEVEL", logging.INFO))


class ExtractionManager:
    producer: Producer
    enricher: EnricherManager
    loader: ElasticLoader
    merger: Merger
    transformer: Transformer
    index_name: str

    def execute_etl(self):
        logging.debug(f"Executing etl for {self.index_name}...")
        for table, changed in self.producer.scan(settings.tables_for_scan):
            logging.debug(f"Produced keys for table {table}")
            try:
                for keys in self.enricher.enrich(table, changed):
                    logging.debug(
                        f"Enriching: table {table}, (index {self.index_name})"
                    )
                    for entries in self.merger.merge(keys):
                        logging.debug(
                            f"Merging: table {table}, (index {self.index_name})"
                        )
                        documents = self.transformer.transform(entries)
                        logging.debug(
                            f"Trasformed: table {table}, (index {self.index_name})"
                        )
                        self.loader.load(documents)
                else:
                    self.producer.set_state(table, self.index_name)
            except Error as e:
                logging.error(e)
                continue
        logging.info(f"Finished etl for {self.index_name}")


class ExtractionMoviesManager(ExtractionManager):
    def __init__(
        self,
        postgres: PostgresConnectionManager,
        redis: RedisConnectionManager,
        elastic: ElasticConnectionManager,
        index_name: str,
    ):
        self.postgres = postgres
        self.redis = redis
        self.elastic = elastic
        self.storage = RedisStorage(self.redis)
        self.state = State(self.storage)
        self.producer = PostgresProducer(self.state, self.postgres, index_name)
        self.enricher = FilmsEnricherManager(self.postgres)
        self.merger = FilmWorkPostgresMerger(self.postgres)
        self.transformer = FilmWork2MoviesTransformer()
        self.loader = ElasticLoader(elastic, index_name)
        self.index_name = index_name


class ExtractionGenresManager(ExtractionManager):
    def __init__(
        self,
        postgres: PostgresConnectionManager,
        redis: RedisConnectionManager,
        elastic: ElasticConnectionManager,
        index_name: str,
    ):
        self.postgres = postgres
        self.redis = redis
        self.elastic = elastic
        self.storage = RedisStorage(self.redis)
        self.state = State(self.storage)
        self.producer = PostgresProducer(self.state, self.postgres, index_name)
        self.enricher = GenreEnricherManager(self.postgres)
        self.merger = GenrePostgresMerger(self.postgres)
        self.transformer = GenreTransformer()
        self.loader = ElasticLoader(elastic, index_name)
        self.index_name = index_name


class ExtractionPersonManager(ExtractionManager):
    def __init__(
        self,
        postgres: PostgresConnectionManager,
        redis: RedisConnectionManager,
        elastic: ElasticConnectionManager,
        index_name: str,
    ):
        self.postgres = postgres
        self.redis = redis
        self.elastic = elastic
        self.storage = RedisStorage(self.redis)
        self.state = State(self.storage)
        self.producer = PostgresProducer(self.state, self.postgres, index_name)
        self.enricher = PersonEnricherManager(self.postgres)
        self.merger = PersonPostgresMerger(self.postgres)
        self.transformer = PersonTransformer()
        self.loader = ElasticLoader(elastic, index_name)
        self.index_name = index_name


def transfer():
    """Основной метод загрузки данных из Postgres в ElasticSearch"""

    logging.debug("Beginning the extraction process...")
    logging.debug("Trying to establsh connection with db...")

    managers = (
        (settings.elastic_movie_index, ExtractionMoviesManager),
        (settings.elastic_genre_index, ExtractionGenresManager),
        (settings.elastic_person_index, ExtractionPersonManager),
    )

    while True:
        try:
            for index, extractor in managers:
                elastic_manager = ElasticConnectionManager(ElasticConnector())
                postgres_manager = PostgresConnectionManager(PostgresConnector())
                redis_manager = RedisConnectionManager(RedisConnector())
                with (
                    elastic_manager as elastic,
                    postgres_manager as postgres,
                    redis_manager as redis,
                ):
                    logging.debug(f"Scanning for index {index}")
                    manager = extractor(postgres, redis, elastic, index)
                    manager.execute_etl()
                    del manager

            logging.debug("Sleeping...")
            sleep(settings.wait_up_to)

        except Exception as e:
            logging.error(type(e))
            logging.error(e)
            logging.error("Exited scan...")
            return


if __name__ == "__main__":
    transfer()
