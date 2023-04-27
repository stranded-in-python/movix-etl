from datetime import datetime
from uuid import UUID

from pydantic import BaseModel, validator


class Entry(BaseModel):
    id: UUID
    modified: datetime
    _transform_elastic = validator("id", allow_reuse=True)(lambda x: str(x) if x else x)


class CrewMember(BaseModel):
    id: UUID
    full_name: str
    _transform_elastic = validator("id", allow_reuse=True)(lambda x: str(x) if x else x)


class Genre(BaseModel):
    id: UUID
    name: str
    _transform_elastic = validator("id", allow_reuse=True)(lambda x: str(x) if x else x)


class FilmWork(BaseModel):
    id: UUID
    title: str
    description: str | None
    rating: float
    genre: tuple[str, ...]
    actors: tuple[CrewMember, ...]
    directors: tuple[CrewMember, ...]
    writers: tuple[CrewMember, ...]
    genres: tuple[Genre, ...]
    _transform_elastic = validator("id", allow_reuse=True)(lambda x: str(x) if x else x)
