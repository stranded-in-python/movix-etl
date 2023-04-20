import uuid

from typing import Optional
from pydantic import BaseModel


class UUIDMixin(BaseModel):
    id: str = str(uuid.uuid4)


class Genre(BaseModel):
    name: str = ''


class Person(UUIDMixin):
    name:  str = ''


class FilmWork(UUIDMixin):
    imdb_rating:    Optional[float]
    genre:          tuple[str, ...] = ()
    title:          Optional[str]
    description:    Optional[str]
    director:       tuple[str, ...] = ()
    actors_names:   tuple[str, ...] = ()
    actors:         tuple[dict, ...] = ()
    writers_names:  tuple[str, ...] = ()
    writers:        tuple[dict, ...] = ()
