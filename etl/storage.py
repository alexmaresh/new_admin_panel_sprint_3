import abc
import logging
from typing import Any
import json
import os
from config import app_config

class BaseStorage:
    @abc.abstractmethod
    def save_state(self, state: dict) -> None:
        """Сохранить состояние в постоянное хранилище"""
        pass

    @abc.abstractmethod
    def retrieve_state(self) -> dict:
        """Загрузить состояние локально из постоянного хранилища"""
        pass


class JsonFileStorage(BaseStorage):
    def __init__(self):
        self.file_path = app_config.storage_file_path

    def check_present(self):
        try:
            with open(self.file_path, 'r'):
                pass
        except FileNotFoundError:
            with open(self.file_path, 'w'):
                pass

    def save_state(self, state):
        self.check_present()
        if os.path.isfile(self.file_path):
            with open(self.file_path, 'r') as f:
                file_data = f.read()
                if not file_data:
                    file_data = state
                else:
                    file_data = json.loads(file_data)
                    file_data = {**file_data, **state}
            with open(self.file_path, 'w') as f:
                f.write(json.dumps(file_data, indent=4, sort_keys=True, default=str))

    def retrieve_state(self):
        self.check_present()
        if os.path.isfile(self.file_path):
            with open(self.file_path, 'r') as f:
                file_data = f.read()
                if not file_data:
                    return {}
                try:
                    return json.loads(file_data)
                except Exception as e:
                    logging.exception(e)



class State:
    """
    Класс для хранения состояния при работе с данными, чтобы постоянно не перечитывать данные с начала.
    Здесь представлена реализация с сохранением состояния в файл.
    В целом ничего не мешает поменять это поведение на работу с БД или распределённым хранилищем.
    """

    def __init__(self, storage: JsonFileStorage):
        self.storage = storage

    def set_state(self, key: str, value: Any) -> None:
        st = {key:value}
        self.storage.save_state(st)

    def get_state(self, key: str) -> Any:
        res = self.storage.retrieve_state()
        state = res.get(key)
        return state