from datetime import datetime

from pydantic import BaseSettings, Field


class Settings(BaseSettings):
    host: str       = Field(
        default='localhost',
        env='REDIS_HOST',
    )
    port: int       = Field(
        default=6379,
        env='REDIS_PORT',
    )
    db: int         = Field(
        default=0,
        env='REDIS_DB',
    )
    retries: int    = Field(
        default=-1,
        env='REDIS_RETRIES',
    )
    dataformat: str = Field(
        default='%Y-%m-%d %H:%M:%S.%f',
        env='REDIS_DATAFORMAT',
    )

    class Config:
        env_file = '.env'
        env_file_encoding = 'utf-8'
        arbitrary_types_allowed = True

    def get_datetime(self, value: str) -> datetime:
        return datetime.strptime(value, self.dataformat)

    def get(self, key: str, default=None):
        return getattr(self, key, default)
