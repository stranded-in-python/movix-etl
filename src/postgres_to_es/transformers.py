from abc import ABC, abstractmethod
from typing import Any, Iterable, Mapping

from .exceptions import DataInconsistentError
from .models import FilmWork


class Transformer(ABC):
    @abstractmethod
    def transform(self, entries: Iterable) -> Iterable:
        ...


class FilmWork2MoviesTransformer(Transformer):
    def person_names(self, persons: Iterable[Mapping[str, str]]):
        return tuple(person["full_name"] for person in persons)

    def transform_single(self, entry: FilmWork) -> dict[str, Any]:
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

    def transform(self, entries: Iterable[FilmWork]) -> Iterable[Mapping[str, Any]]:
        return [self.transform_single(entry) for entry in entries]
