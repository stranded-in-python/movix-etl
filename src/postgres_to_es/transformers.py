from abc import ABC, abstractmethod
from typing import Any, Iterable, Mapping

from pydantic import BaseModel

from .exceptions import DataInconsistentError
from .models import FilmWorkDocument, GenreDocument, PersonDocument


class Transformer(ABC):
    @abstractmethod
    def transform(self, entries: Iterable) -> Iterable:
        ...


class SingleTransformer(Transformer, ABC):
    @abstractmethod
    def transform_single(self, entry: BaseModel) -> dict[str, Any]:
        ...

    def transform(
        self, entries: Iterable[FilmWorkDocument]
    ) -> Iterable[Mapping[str, Any]]:
        return [self.transform_single(entry) for entry in entries]


class FilmWork2MoviesTransformer(SingleTransformer):
    def person_names(self, persons: Iterable[Mapping[str, str]]):
        return tuple(person["full_name"] for person in persons)

    def transform_single(self, entry: FilmWorkDocument) -> dict[str, Any]:
        state = entry.dict()
        try:
            state["imdb_rating"] = state.get("rating")
            del state["rating"]

            state["director"] = self.person_names(state["directors"])
            state["actors_names"] = self.person_names(state["actors"])
            state["writers_names"] = self.person_names(state["writers"])
        except KeyError or TypeError as e:
            raise DataInconsistentError(
                f"Failed to transform entry {state}." f" Error msg: {e}"
            )
        return state


class GenreTransformer(SingleTransformer):
    def transform_single(self, entry: GenreDocument) -> dict[str, Any]:
        return entry.dict()


class PersonTransformer(SingleTransformer):
    def transform_single(self, entry: PersonDocument) -> dict[str, Any]:
        return entry.dict()
