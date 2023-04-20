from datetime import datetime
from uuid import UUID

from pydantic import BaseModel


class Entry(BaseModel):
    modified: datetime
    id: UUID


class CrewMember(BaseModel):
    id: UUID
    name: str


class FilmWork(BaseModel):
    id: UUID
    title: str
    description: str = ""
    rating: float
    genre: list[str]
    actors: list[CrewMember]
    directors: list[CrewMember]
    writers: list[CrewMember]
