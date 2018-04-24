'''Поддержка операций с базой данных MongoDB.'''

__author__ = 'Gennady Kovalev <gik@bigur.ru>'
__copyright__ = '(c) 2016-2018 Business group for development management'
__licence__ = 'For license information see LICENSE'

from urllib.parse import urlparse

from motor.core import AgnosticBaseProperties
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorCursor, AsyncIOMotorDatabase, AsyncIOMotorCollection

from bigur.config import config
from bigur.utils import class_by_name


def compile_object(document):
    '''Превращает документ, полученный из базы в объект python.'''

    if document is not None and '_class' in document:
        cls = class_by_name(document['_class'])
        obj = cls.__new__(cls)
        obj.__setstate__(document)
        return obj
    return document


class DBProxy(object):
    def __init__(self, section, param, fallback=None):
        self._section = section
        self._param = param
        self._fallback = fallback
        self._db = None

    @property
    def origin(self):
        return self._db

    def _configure(self):
        uri = config.get(self._section, self._param, fallback=self._fallback)
        db_name = urlparse(uri).path.strip('/')
        self._db = Client(uri)[db_name]

    def __getattr__(self, key):
        if self._db is None:
            self._configure()
        return getattr(self._db, key)

    def __getitem__(self, key):
        if self._db is None:
            self._configure()
        return self._db[key]


class Client(AsyncIOMotorClient):
    '''Обёртка вокруг :class:`~AsyncIOMotorClient`. Нужна для возвращения
    нашего объекта с базой данных.'''

    def __getitem__(self, name):
        return Database(self, name)


class Database(AsyncIOMotorDatabase):
    '''Обёртка вокруг :class:`~AsyncIOMotorDatabase`. Нужна для возвращения
    нашего объекта с коллекцией.'''

    def __init__(self, client, name, _delegate=None):
        self._client = client
        delegate = _delegate or self.__delegate_class__(client.delegate, name)
        super(AgnosticBaseProperties, self).__init__(delegate) # pylint: disable=E1003

    def __getitem__(self, name):
        return Collection(self, name)


class Collection(AsyncIOMotorCollection):
    '''Обёртка вокруг коллекции.'''

    def __init__(self, database, name, _delegate=None):
        self.database = database
        delegate = _delegate or self.__delegate_class__(database.delegate, name)
        super(AgnosticBaseProperties, self).__init__(delegate) # pylint: disable=E1003

    async def find_one(self, *args, **kwargs):
        '''Получение одного объекта.'''

        return compile_object((await super().find_one(*args, **kwargs)))

    def find(self, *args, **kwargs):
        '''Возвращает :class:`~.Cursor` для итерации.'''
        return Cursor(self.delegate.find(*args, **kwargs), self)


class Cursor(AsyncIOMotorCursor):
    '''Обёртка вокруг курсора.'''

    def next_object(self):
        '''Получение документа из курсора.'''
        return compile_object(super().next_object())


db = DBProxy('general', 'database_uri', 'mongodb://localhost/test')
