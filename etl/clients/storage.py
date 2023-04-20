from abc import abstractmethod
from typing import Any, Dict, TypeVar

FieldTypes = TypeVar('FieldTypes', int, str, bool)
StateFields = Dict[str, FieldTypes]


class BaseStorage:
    @abstractmethod
    def save_state(self, state: dict) -> None:
        """Сохранить состояние в постоянное хранилище"""
        pass

    @abstractmethod
    def retrieve_state(self) -> dict:
        """Загрузить состояние локально из постоянного хранилища"""
        pass


class State:
    """
    Класс для хранения состояния при работе с данными.
    """

    def __init__(self, storage: BaseStorage, initial_state: StateFields = None):
        self.storage: BaseStorage = storage
        if initial_state is not None:
            self.state = dict(initial_state)
        else:
            self.state = {}
        self.retrieve()

    def set(self, key: str, value: Any) -> None:
        """Установить состояние для определённого ключа"""
        self.state[key] = value  

    def get(self, key: str) -> Any:
        """Получить состояние по определённому ключу"""
        return self.state.get(key, None)

    def save(self) -> None:
        self.storage.save_state(self.state)

    def retrieve(self) -> Dict:
        self.state.update(self.storage.retrieve_state())

        return self.state
