from typing import Iterable

from pydantic import BaseSettings


class Settings(BaseSettings):
    elastic_endpoint: str
    elastic_index: str = "movies"
    elastic_pack_size: int = 1000

    postgres_db: str
    postgres_user: str
    postgres_password: str
    postgres_host: str
    postgres_port: str

    redis_host: str
    redis_port: int

    tables_for_scan: Iterable[tuple[str, int]] = (
        ("content.genre", 1),
        ("content.person", 1000),
        ("content.film_work", 1000),
        ("content.genre_film_work", 1000),
        ("content.person_film_work", 1000),
    )

    wait_up_to: int = 60 * 60 * 12
    waiting_interval: int = 60 * 30
    waiting_factor: int = 2
    first_nap: float = 0.1


settings = Settings()  # type: ignore
