from datetime import datetime
from typing import Optional, List

import uuid
from pydantic import BaseModel
from pydantic.fields import Field


class Person(BaseModel):

    id: str
    name: Optional[str]

class Genre(BaseModel):
    id: uuid.UUID = Field(default_factory=uuid.uuid4)
    name: str

class FilmWorkID(BaseModel):

    id: uuid.UUID = Field(default_factory=uuid.uuid4)
    updated_at: datetime

class PSQLFilmwork(BaseModel):

    fw_id: uuid.UUID = Field(default_factory=uuid.uuid4)
    title: str
    description: Optional[str]
    rating: Optional[float]
    type: str
    created_at: datetime
    updated_at: datetime
    role: Optional[str]
    person_id: Optional[str] = Field(alias='person_id')
    person_name: Optional[str] = Field(alias='full_name')
    genre_name: Optional[str] = Field(alias='name')

class ESFilmwork(BaseModel):
    
    id: uuid.UUID = Field(default_factory=uuid.uuid4)
    imdb_rating: Optional[float]
    genre: List[str]
    title: str
    description: Optional[str]
    director: List[str]
    actors_names: List[str]
    writers_names: List[str]
    actors: List[Person]
    writers: List[Person]