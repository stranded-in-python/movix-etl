from abc import ABC, abstractmethod
from typing import Any, Iterable, Mapping

from .connections import ConnectionManager, ElasticConnectionManager


class Loader(ABC):
    def __init__(self, manager: ConnectionManager):
        self.manager = manager

    @abstractmethod
    def load(self, items: Iterable):
        ...


class ElasticLoader(Loader):
    def __init__(self, manager: ElasticConnectionManager, index_name: str):
        super().__init__(manager)
        self.manager = manager
        self.index_name = index_name

    def _prepare_operations(self, items: Iterable[Mapping[str, Any]]):
        return [
            {"_op_type": "index", "_index": self.index_name, "_id": item["id"], **item}
            for item in items
        ]

    def load(self, items: Iterable[Mapping[str, Any]]):
        operations = self._prepare_operations(items)
        self.manager.bulk(operations)
