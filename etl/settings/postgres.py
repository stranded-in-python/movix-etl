from pydantic import BaseSettings, Field


class Tables(BaseSettings):
    filmwork: str           = Field(
        default='content.film_work',
        env='FILMWORK_TABLE_NAME',
    )
    person: str             = Field(
        default='content.person',
        env='PERSON_TABLE_NAME',
    )
    genre: str              = Field(
        default='content.genre',
        env='GENRE_TABLE_NAME',
    )

    def get(self, key: str, default=None):
        return getattr(self, key, default)


class Settings(BaseSettings):
    name: str       = Field(
        default='movies_database',
        env='POSTGRES_NAME',
    )
    user: str       = Field(
        default='app',
        env='POSTGRES_USER',
    )
    password: str   = Field(
        default='123qwe',
        env='POSTGRES_PASSWORD',
    )
    host: str       = Field(
        default='localhost',
        env='POSTGRES_HOST',
    )
    port: int       = Field(
        default=5432,
        env='POSTGRES_PORT',
    )
    tables: Tables  = Field(
        default_factory=Tables
    )

    class Config:
        env_file = '.env'
        env_file_encoding = 'utf-8'
        arbitrary_types_allowed = True

    def get(self, key: str, default=None):
        return getattr(self, key, default)
