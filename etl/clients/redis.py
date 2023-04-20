import json

from logging import Logger
from typing import Any, Iterable

from redis import Redis
from redis.backoff import ExponentialBackoff
from redis.exceptions import (
    BusyLoadingError, ConnectionError, TimeoutError,
)
from redis.retry import Retry

from etl.clients.storage import BaseStorage
from etl.settings.redis import Settings


class RedisStorage(BaseStorage):

    def __init__(self, adapter: Redis, state_name: str = 'state'):
        self.adapter = adapter
        self._state_name = state_name

    def get_all(self) -> dict:
        """Получить все данные хранилища"""
        keys = self.adapter.scan_iter()
        return {key: json.load(value) for key, value in zip(
            keys,
            self.adapter.get(keys=keys, decode_responses=True, ),
        )
                }

    def get_key(self, key: str) -> Any:
        """Получить значение ключа из хранилища"""
        return self.adapter.get(key)

    def set_key(self, key: str, value) -> None:
        """Установить значение ключа в хранилище"""
        self.adapter.set(key, value)

    def del_key(self, *keys: Iterable[str]) -> None:
        """Удалить ключи в хранилище"""
        self.adapter.delete(*keys)

    def save_state(self, state: dict) -> None:
        """Сохранить состояние в постоянное хранилище"""
        self.set_key(self._state_name, json.dumps(state))

    def retrieve_state(self) -> dict:
        """Получить состояние из постоянного хранилище"""
        state = self.get_key(self._state_name)
        if state is not None:
            return json.loads(state)
        return {}


def create_redis_adapter(
        settings: Settings,
        logger: Logger,
) -> Redis:
    """Создание базового клиента-адаптера Redis с бэкоффом и .."""
    logger.info('Creating Redis client..')
    redis_client = Redis(
        host=settings.get('host'),
        port=settings.get('port'),
        db=settings.get('db'),
        decode_responses=True,
        retry=Retry(
            backoff=ExponentialBackoff(),
            retries=settings.get('retries'),
        ),
        retry_on_error=[
            BusyLoadingError,
            ConnectionError,
            TimeoutError, ConnectionRefusedError
        ],
    )
    logger.info('Redis client was created.')

    logger.info('Redis client connecting to a database..')
    if redis_client.ping():
        logger.info('Redis client has connected to the database.')
    else:
        raise ConnectionError()

    return redis_client
