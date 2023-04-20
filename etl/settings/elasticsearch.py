from typing import TypeVar, Mapping, Any

from pydantic import BaseSettings


IndexSettings = TypeVar('IndexSettings', Mapping[str, Any], None)
'''
Тип данных, представляющий настройки и маппинг индекса,
которые передаются в тело запроса на его создание
'''


class ClientSettings(BaseSettings):
    debug: bool         = False
    host: str           = 'localhost'
    port: int           = 9200
    api_key: str        = ''
    scheme: str         = 'movies'
    scheme_path: str    = 'es_schema.txt'

    class Config:
        env_file = '.env'
        env_prefix = 'ELASTICSEARCH_'

    def get(self, key: str, default=None):
        return getattr(self, key, default)
