'''Database document.'''

__author__ = 'Gennady Kovalev <gik@bigur.ru>'
__copyright__ = '(c) 2016-2018 Business group for development management'
__licence__ = 'For license information see LICENSE'

from dataclasses import dataclass
from datetime import datetime, timezone
from importlib import import_module
from logging import getLogger
from sys import modules
from typing import Dict, Any, Set, Optional, List, TypeVar, Iterable

from bson import DBRef, ObjectId
from pymongo.results import InsertOneResult, UpdateResult, DeleteResult

from bigur.store.typing import Document as DocumentType
from bigur.store.database import Collection, Cursor
from bigur.store.database import db
from bigur.store.lazy_ref import LazyRef
from bigur.store.unit_of_work import context

logger = getLogger(__name__)

T = TypeVar('T')


def pickle(obj: Any) -> Any:
    '''Transform object to MongoDB document.'''
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
    '''Transform MongoDB document to object.'''

    unpickled: Any

    if isinstance(obj, datetime) and obj.tzinfo is None:
        unpickled = obj.replace(tzinfo=timezone.utc)

    elif isinstance(obj, list):
        unpickled = EmbeddedList()
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
        unpickled = EmbeddedDict()
        for key, value in obj.items():
            unpickled[key] = unpickle(value)

    elif isinstance(obj, DBRef):
        unpickled = LazyRef(obj)

    else:
        unpickled = obj

    return unpickled


@dataclass(init=False)
class Node:
    '''Abstract node for recursivity support.'''

    def __post_init__(self) -> None:
        logger.debug('Node.__init__ (%s) start', self)
        self.__node_parent__: Optional[Document] = None
        self.__node_name__: Optional[str] = None
        logger.debug('Node.__init__ (%s) end', self)

    def mark_dirty(self, keys: Set[str]) -> None:
        '''Mark root node as dirty.'''
        parent = getattr(self, '__node_parent__', None)
        if parent is not None:
            name = self.__node_name__
            parent_keys = {'{}.{}'.format(name, x) for x in keys}
            parent.mark_dirty(parent_keys)


@dataclass(init=False)
class EmbeddedList(Node, List[T]):
    '''List that stored in database'''

    def __post_init__(self, iterable: Iterable = ()) -> None:
        # pylint: disable=E1003
        super(EmbeddedList, self).__init__()
        super(Node, self).__init__(iterable)  # type: ignore


@dataclass(init=False)
class EmbeddedDict(Node, Dict[str, Any]):
    '''Dict that stored in database'''


@dataclass(init=False)
class Document(DocumentType, Node):
    '''Abstract database document.'''

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
                obj = picklers[key]['unpickle'](self, state, value)
            else:
                obj = unpickle(value)
            state[key] = obj
            if isinstance(obj, Node):
                obj.__node_parent__ = self
                obj.__node_name__ = key

        self.__dict__.update(state)

    def __setattr__(self, key: str, value: Any) -> None:
        logger.debug('Document.__setattr__ (%s) set %s=%s', self, key, value)

        super().__setattr__(key, value)

        if key not in ('__node_parent__', '__node_name__'):
            if isinstance(value, Node):
                value.__node_parent__ = self
                value.__node_name__ = key

            parent = getattr(self, '__node_parent__', None)
            if parent:
                parent.mark_dirty({'{}.{}'.format(self.__node_name__, key)})

    def __getattr__(self, key: str) -> Any:
        if key in self.__dict__:
            return self.__dict__[key]


@dataclass(init=False)
class Embedded(Document):
    '''Embedded database document.'''


@dataclass
class Stored(Document):
    '''Root database document.'''

    def __post_init__(self, id_: Optional[ObjectId] = None) -> None:
        logger.debug('Stored.__post_init__ (%s) start', self)
        if id_ is None:
            id_ = ObjectId()
        self._id: ObjectId = id_

        self.mark_new()

        self.__unit_of_work__ = context.get()

        super().__post_init__()
        logger.debug('Stored.__post_init__ (%s) end', self)

    @property
    def id(self):
        return self._id

    def __setattr__(self, key: str, value: Any):
        logger.debug('Stored.__setattr__ (%s) set %s=%s', self, key, value)
        super().__setattr__(key, value)
        if key not in ('__unit_of_work__',
                       '__node_parent__',
                       '__node_name__') \
                and '__unit_of_work__' in self.__dict__:
            self.mark_dirty(keys={key})

    # Регистрация объектов в UnitOfWork
    def mark_new(self) -> None:
        '''Mark object as new.'''
        uow = context.get()
        if uow is not None:
            logger.debug('Mark object %s as new', self)
            uow.register_new(self)
        else:
            logger.warning(
                'Creating object without db context.', stack_info=True)

    def mark_dirty(self, keys: Set[str]) -> None:
        '''Mark object as dirty.'''
        if getattr(self, '_id', None) is not None:
            uow = context.get()
            if uow is not None:
                uow.register_dirty(self, keys)
            else:
                logger.warning(
                    'Changing object without database context.',
                    stack_info=True)

    def mark_removed(self) -> None:
        '''Mark document for removal.'''
        if getattr(self, '_id', None) is not None:
            logger.debug('Marking object %s for deletion', self)
            uow = context.get()
            if uow is not None:
                uow.register_removed(self)

    # Collection
    @classmethod
    def get_collection(cls) -> Collection:
        '''Returns MongoDB collection for this class.'''
        name = cls.__metadata__.get('collection')
        if name is None:
            name = str(cls.__name__).lower()
        return db[name]

    # Запрос объектов из базы данных
    @classmethod
    def find(cls, query: dict) -> Cursor:
        '''Возвращает курсор для перебора объектов.'''
        return cls.get_collection().find(query)

    @classmethod
    async def find_one(cls, query: dict) -> Cursor:
        '''Возвращает один объект из БД, удовлетворяющий условиям
        поиска `query`, или None.'''
        return await cls.get_collection().find_one(query)

    # Изменение объектов
    @classmethod
    async def insert_one(cls, document: 'Stored') -> InsertOneResult:
        '''Вставляет документ в базу данных.'''
        collection = cls.get_collection()
        state = document.__getstate__()
        return await collection.insert_one(state)

    @classmethod
    async def update_one(cls,
                         document: 'Stored',
                         keys: Optional[Set[str]] = None) -> UpdateResult:
        '''Обновляет документ в базу данных. Если указаны keys, то
        обновление происходит через `update_one`, иначе через
        `replace_one`.'''
        collection = cls.get_collection()

        state = document.__getstate__()

        if keys:
            update: Dict[str, Any] = {}
            remove: Dict[str, None] = {}
            for key in keys:
                path = key.split('.')
                obj: Any = state
                for attr in path:
                    obj = obj.get(attr)
                if obj is None:
                    remove[key] = obj
                else:
                    update[key] = obj

            query: dict = {}
            if update:
                query['$set'] = update
            if remove:
                query['$unset'] = remove
            logger.debug('Запрос на обновление: %s', query)

            return await collection.update_one({'_id': document.id}, query)
        else:
            return collection.replace_one({'_id': document.id}, state)

    @classmethod
    async def delete_one(cls, document: 'Stored') -> DeleteResult:
        '''Удаляет `document` из базы данных.'''
        return await cls.get_collection().delete_one({'_id': document.id})

    # Удаление объекта
    async def remove(self):
        '''Помечает объект на удаление.'''
        self.mark_removed()
