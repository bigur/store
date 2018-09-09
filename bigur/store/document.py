'''Документ данных.'''

__author__ = 'Gennady Kovalev <gik@bigur.ru>'
__copyright__ = '(c) 2016-2018 Business group for development management'
__licence__ = 'For license information see LICENSE'

from datetime import datetime, timezone
from importlib import import_module
from logging import getLogger
from sys import modules
from typing import Dict, Any, Set, Optional
from uuid import uuid4
from warnings import warn

from bson.dbref import DBRef

from bigur.store.typing import Id, Document as DocumentType
from bigur.store.database import DBProxy, db
from bigur.store.lazy_ref import LazyRef
from bigur.store.unit_of_work import context


logger = getLogger('bigur.store.document')


def pickle(obj: Any) -> Any:
    '''Преобразования объекта в документ MongoDB.'''
    pickled: Any

    if isinstance(obj, (str, int, float, bool, bytes)):
        pickled = obj

    elif isinstance(obj, list):
        pickled = []
        for value in obj:
            pickled.append(pickle(value))

    elif isinstance(obj, dict):
        pickled = {}
        for key in iter(obj):
            pickled[key] = pickle(obj[key])

    elif isinstance(obj, LazyRef):
        pickled = obj.dbref

    elif isinstance(obj, Embedded):
        pickled = obj.__getstate__()

    elif isinstance(obj, Stored):
        collection = type(obj).get_collection().name
        pickled = DBRef(collection, obj.id)

    else:
        pickled = obj

    return pickled


def unpickle(obj: Any) -> Any:
    '''Преобразования объекта MongoDB в документ.'''
    unpickled = obj

    if isinstance(obj, datetime) and obj.tzinfo is None:
        unpickled = obj.replace(tzinfo=timezone.utc)

    elif isinstance(obj, list):
        unpickled = List
        for value in obj:
            unpickled.append(unpickle(value))

    elif isinstance(obj, dict) and '_class' in obj:
        splitted = obj['_class'].split('.')
        class_name = splitted.pop()
        module_name = '.'.join(splitted)
        try:
            module = modules[module_name]
        except KeyError:
            module = import_module(module_name)
        cls = getattr(module, class_name)
        unpickled = cls.__new__(cls)
        unpickled.__setstate__(obj)

    elif isinstance(obj, dict):
        unpickled = Embedded
        for key, value in obj.items():
            unpickled[key] = unpickle(value)

    elif isinstance(obj, DBRef):
        unpickled = LazyRef(obj)

    return unpickled


class Node(object):
    '''Узел для поддержки вложенности документов.'''
    def __init__(self) -> None:
        self.__node_parent__: Optional[Document] = None
        self.__node_name__: Optional[str] = None
        super().__init__()


class List(Node, list):
    '''Список, который является свойством документа БД.'''


class Document(DocumentType, Node):
    '''Абстрактный документ базы данных.'''

    # Изменение состояния объекта
    def __getstate__(self) -> Dict[str, Any]:
        metadata = type(self).__metadata__
        include = metadata.get('include_attrs', [])
        exclude = metadata.get('exclude_attrs', [])
        replace = metadata.get('replace_attrs', {})
        picklers = metadata.get('picklers', {})

        cls = type(self)
        state = {'_class': '{}.{}'.format(cls.__module__, cls.__name__)}
        for attr in self.__dict__:
            key = attr
            if key in replace:
                key = replace[key]
            if key.startswith('_') and key not in include and key != '_id':
                continue
            elif key in exclude:
                continue
            if attr in picklers:
                value = picklers[attr]['pickle'](self, getattr(self, attr))
            else:
                value = pickle(getattr(self, attr))
            if value is not None:
                state[key] = value
        return state

    def __setstate__(self, data: Dict[str, Any], recurse=True):
        replace = self.__metadata__.get('replace_attrs', {})
        replaced = dict([(v, k) for k, v in replace.items()])
        picklers = self.__metadata__.get('picklers', {})

        state = {'_saved': True}
        for key, value in data.items():
            if key in replaced:
                key = replaced[key]
            if key in picklers:
                state[key] = picklers[key]['unpickle'](self, state, value)
            else:
                state[key] = unpickle(value)

        self.__dict__.update(state)

    # Установка атрибутов
    def __setattr__(self, key: str, value: Any) -> None:
        super().__setattr__(key, value)
        if key not in ('__node_parent__', '__node_name__'):
            if isinstance(value, Node):
                value.__node_parent__ = self
                value.__node_name__ = key


class Embedded(Document):
    '''Объект, который является встроенным документом в другой.'''

    #def __setattr__(self, key: str, value: Any):
    #    super().__setattr__(key, value)
    #    parent = getattr(self, '__node_parent__', None)
    #    if parent:
    #        parent._mark_dirty({'{}.{}'.format(self.__node_name__, key)})


class Stored(Document):
    '''Объект, который сохраняется в базу данных.'''

    def __init__(self, id_: Optional[Id] = None) -> None:

        if id_ is None:
            id_ = Id(str(uuid4()))
        self._id: Id = id_

        self._mark_new()

        self.__unit_of_work = context.get()

        super().__init__()

    @property
    def id(self): # pylint: disable=invalid-name
        '''Возвращает идентификатор объекта, хранящийся в `self._id`.'''
        return self._id

    # Установка атрибутов
    def __setattr__(self, key: str, value: Any):
        super().__setattr__(key, value)
        if key != '__unit_of_work' and hasattr(self, '__unit_of_work'):
            self._mark_dirty(keys={key})

    # Регистрация объектов в UnitOfWork
    def _mark_new(self) -> None:
        uow = context.get()
        if uow is not None:
            logger.debug('Помечаю объект %s как новый', self)
            uow.register_new(self)
        else:
            logger.warning('Создаю объект вне контекста БД.', stack_info=True)

    def _mark_dirty(self, keys: Set[str]) -> None:
        if getattr(self, '_id', None) is not None:
            logger.debug(
                'Помечаю объект %s как грязный, атрибуты %s',
                self,
                ','.join(keys)
            )
            uow = context.get()
            if uow is not None:
                uow.register_dirty(self, keys)
            else:
                logger.warning('Изменяю объект вне контекста БД.', stack_info=True)

    def _mark_removed(self) -> None:
        pass

    # Коллекция
    @classmethod
    def get_collection(cls):
        meta = cls.__metadata__
        if 'dbconfig' in meta:
            # XXX: на каждый класс создаётся свой Proxy! Надо сделать один.
            dbase = DBProxy(meta['dbconfig']['section'],
                            meta['dbconfig']['param'])
        else:
            dbase = db

        name = meta.get('collection')
        if name is None:
            name = str(cls.__name__).lower()
        return dbase[name] # pylint: disable=E1136

    # Запрос объектов из базы данных
    @classmethod
    def find(cls, *args, **kwargs):
        return cls.get_collection().find(*args, **kwargs)

    @classmethod
    async def find_one(cls, *args, **kwargs):
        return await cls.get_collection().find_one(*args, **kwargs)

    # Изменение объектов
    @classmethod
    async def insert_one(cls, *args, **kwargs):
        return await cls.get_collection().insert_one(*args, **kwargs)

    @classmethod
    async def update_one(cls, *args, **kwargs):
        return await cls.get_collection().update_one(*args, **kwargs)

    @classmethod
    async def delete_one(cls, *args, **kwargs):
        return await cls.get_collection().delete_one(*args, **kwargs)

    # Сохранение объекта в БД
    async def save(self):
        '''Сохранение объектов.'''
        warn(DeprecationWarning, 'используйте единицу работы вместо '
                                 'прямой записи объекта.')
        state = self.__getstate__()
        if not getattr(self, '_saved', False):
            await self.insert_one(state)
            self._saved = True
        else:
            unset = {}
            for attr in self.__dict__:
                value = getattr(self, attr)
                if value is None:
                    unset[attr] = None
            if unset:
                query = {'$set': state, '$unset': unset}
            else:
                query = {'$set': state}
            await self.update_one({'_id': self._id}, query)
        return self

    async def remove(self):
        pass
