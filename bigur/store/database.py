'''Поддержка операций с базой данных MongoDB.'''

__author__ = 'Gennady Kovalev <gik@bigur.ru>'
__copyright__ = '(c) 2016-2018 Business group for development management'
__licence__ = 'For license information see LICENSE'

from importlib import import_module
from typing import Union, Optional
from sys import modules
from urllib.parse import urlparse

from motor.core import AgnosticBaseProperties
from motor.motor_asyncio import (AsyncIOMotorClient, AsyncIOMotorCursor,
                                 AsyncIOMotorDatabase, AsyncIOMotorCollection)

from bigur.utils import config

from bigur.store.typing import DatabaseDict, Document
from bigur.store.unit_of_work import context


DocumentOrObject = Union[Document, DatabaseDict]


def compile_object(document: DatabaseDict) -> DocumentOrObject:
    '''Превращает документ, полученный из базы в объект python.'''
    if document is not None and '_class' in document:
        splitted = document['_class'].split('.')
        class_name = splitted.pop()
        module_name = '.'.join(splitted)
        try:
            module = modules[module_name]
        except KeyError:
            module = import_module(module_name)
        cls = getattr(module, class_name)
        obj = cls.__new__(cls)
        obj.__setstate__(document)
        obj.__unit_of_work__ = context.get()
        document = obj

    return document


class Client(AsyncIOMotorClient):
    '''Обёртка вокруг :class:`~AsyncIOMotorClient`. Нужна для возвращения
    нашего объекта с базой данных.'''

    def __getitem__(self, name: str) -> 'Database':
        return Database(self, name)


class Database(AsyncIOMotorDatabase):
    '''Обёртка вокруг :class:`~AsyncIOMotorDatabase`. Нужна для возвращения
    нашего объекта с коллекцией.'''

    def __init__(self, client: Client, name: str, _delegate=None) -> None:
        self._client: Client = client
        delegate = _delegate or self.__delegate_class__(client.delegate, name)
        super(AgnosticBaseProperties, self).__init__(delegate)

    def __getitem__(self, name: str) -> 'Collection':
        return Collection(self, name)


class Collection(AsyncIOMotorCollection):
    '''Обёртка вокруг коллекции.'''

    def __init__(self, database: Database, name: str, _delegate=None) -> None:
        self.database: Database = database
        delegate = _delegate or self.__delegate_class__(
            database.delegate, name)
        super(AgnosticBaseProperties, self).__init__(delegate)

    async def find_one(self, *args, **kwargs) -> DocumentOrObject:
        '''Получение одного объекта.'''
        return compile_object((await super().find_one(*args, **kwargs)))

    async def count_documents(self, *args, **kwargs) -> int:
        '''Получение числа документов, которое будет возвращенго запросом.'''
        return await super().count(*args, **kwargs)

    def find(self, *args, **kwargs) -> 'Cursor':
        '''Возвращает :class:`~.Cursor` для итерации.'''
        return Cursor(self.delegate.find(*args, **kwargs), self)


class Cursor(AsyncIOMotorCursor):
    '''Обёртка вокруг курсора.'''

    def next_object(self) -> DocumentOrObject:
        '''Получение документа из курсора.'''
        return compile_object(super().next_object())


class DBProxy(object):
    def __init__(self, section: str, param: str,
                 fallback: Optional[str] = None) -> None:
        self._section: str = section
        self._param: str = param
        self._fallback: Optional[str] = fallback
        self._db: Optional[Database] = None

    @property
    def origin(self) -> Optional[Database]:
        return self._db

    def _configure(self) -> None:
        url = config.get(self._section, self._param, fallback=self._fallback)
        db_name = urlparse(url).path.strip('/')
        self._db = Client(url)[db_name]

    def __getattr__(self, key):
        if self._db is None:
            self._configure()
        return getattr(self._db, key)

    def __getitem__(self, key):
        if self._db is None:
            self._configure()
        return self._db[key]


db = DBProxy('general', 'database_url', 'mongodb://localhost/test')
