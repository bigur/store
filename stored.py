'''Модуль с поддержкой хранения объектов в базе данных.'''

__author__ = 'Gennady Kovalev <gik@bigur.ru>'
__copyright__ = '(c) 2016-2018 Business group for development management'
__licence__ = 'For license information see LICENSE'

from uuid import uuid4
from datetime import datetime, timezone

from bson.dbref import DBRef

from bigur.utils import AttrDict

from .database import db, DBProxy


class IntegrityError(Exception):
    '''Ошибка загрузки объекта из БД, использую указанную ссылку.'''


class NotResolved(Exception):
    '''Ссылка ещё не достала объект из базы.'''


class LazyRef(object):
    '''Ленивая ссылка. Автоматически подгружает объект из базы данных при обращении к нему.'''
    __own_keys__ = ('_dbref', '_obj')

    def __init__(self, dbref):
        self._dbref = dbref
        self._obj = None

    @property
    def collection(self):
        return self._dbref.collection

    @property
    def id(self):
        return self._dbref.id

    async def resolve(self):
        if self._obj is None:
            dbase = self._dbref.database
            if dbase is None:
                dbase = db
            collection = dbase[self._dbref.collection] # pylint: disable=E1136
            obj = await collection.find_one({'_id': self._dbref.id})
            if obj is None:
                raise IntegrityError('не смог подгрузить объект из коллекции {} с ИД {}'.format(
                    self._dbref.collection, self._dbref.id))
            self._obj = obj
        return self._obj

    def __getattr__(self, key):
        if self._obj is None:
            raise NotResolved('загрузите объект из базы с помощью .resolve()')
        return getattr(self._obj, key)

    def __setattr__(self, key, value):
        if key in self.__own_keys__:
            super().__setattr__(key, value)
        else:
            if self._obj is None:
                raise NotResolved('загрузите объект из базы с помощью <ref>.resolve()')
            setattr(self._obj, key, value)


class MetadataType(type):

    def __init__(cls, name, bases, attrs):
        metadata = {}
        for class_ in reversed(cls.__mro__):
            meta = getattr(class_, '__metadata__', {})
            for key, value in meta.items():
                if key == 'replace_attrs':
                    if 'replace_attrs' not in metadata:
                        metadata['replace_attrs'] = {}
                    metadata['replace_attrs'].update(value)
                elif key == 'picklers':
                    if 'picklers' not in metadata:
                        metadata['picklers'] = {}
                    metadata['picklers'].update(value)
                else:
                    metadata[key] = value
        cls.__metadata__ = metadata


class Stored(AttrDict, metaclass=MetadataType):

    __metadata__ = {
        'collection': None,
        'include_attrs': ['_id'],
        'exclude_attrs': ['_saved'],
        'replace_attrs': {},
    }

    __events__ = ['created', 'changed', 'deleted']

    def __init__(self, _id=None):
        if _id is None:
            _id = str(uuid4())
        self._id = _id

        self._saved = False

        super().__init__()

    @property
    def id(self): #pylint: disable=invalid-name
        '''Возвращает идентификатор объекта, хранящийся в `self._id`.'''
        return self._id

    @classmethod
    def get_collection(cls):
        meta = cls.__metadata__
        if 'dbconfig' in meta:
            dbase = DBProxy(meta['dbconfig']['section'], meta['dbconfig']['param'])
        else:
            dbase = db

        name = meta.get('collection')
        if name is None:
            name = str(cls.__name__).lower()
        return dbase[name] # pylint: disable=E1136

    @classmethod
    def find(cls, *args, **kwargs):
        return cls.get_collection().find(*args, **kwargs)

    @classmethod
    async def find_one(cls, *args, **kwargs):
        return await cls.get_collection().find_one(*args, **kwargs)

    @classmethod
    async def insert_one(cls, *args, **kwargs):
        return await cls.get_collection().insert_one(*args, **kwargs)

    @classmethod
    async def update_one(cls, *args, **kwargs):
        return await cls.get_collection().update_one(*args, **kwargs)

    @classmethod
    async def remove(cls, *args, **kwargs):
        return await cls.get_collection().remove(*args, **kwargs)

    async def save(self):
        '''Сохраняет объект в базе данных. Если объект уже был создан,
        то обновляет его.'''
        state = self.__getstate__()
        if not getattr(self, '_saved', False):
            await self.insert_one(state)
            self._saved = True
        else:
            unset = {}
            for attr in self:
                value = self[attr]
                if value is None:
                    unset[attr] = None
            if unset:
                query = {'$set': state, '$unset': unset}
            else:
                query = {'$set': state}
            await self.update_one({'_id': self._id}, query)
        return self

    @classmethod
    def _pickle(cls, obj):
        if isinstance(obj, list):
            pickled = []
            for value in obj:
                pickled.append(cls._pickle(value))

        elif isinstance(obj, Stored):
            collection = type(obj).get_collection().name
            pickled = DBRef(collection, obj.id)

        elif isinstance(obj, (str, int, float, bool, bytes)):
            pickled = obj

        elif hasattr(obj, '__iter__'):
            with_keys = hasattr(obj, '__getitem__')
            if with_keys:
                pickled = {}
            else:
                pickled = []
            for key in iter(obj):
                if with_keys:
                    pickled[key] = cls._pickle(obj[key])
                else:
                    pickled.append(cls._pickle(obj[key]))
        else:
            pickled = obj

        return pickled

    @classmethod
    def _unpickle(cls, obj):
        unpickled = obj

        if isinstance(obj, DBRef):
            unpickled = LazyRef(obj)

        elif isinstance(obj, datetime):
            unpickled = obj.replace(tzinfo=timezone.utc)

        elif isinstance(obj, list):
            unpickled = []
            for value in obj:
                unpickled.append(cls._unpickle(value))

        elif isinstance(obj, dict):
            unpickled = {}
            for key, value in obj.items():
                unpickled[key] = cls._unpickle(value)

        return unpickled

    def __getstate__(self):
        metadata = type(self).__metadata__
        include = metadata.get('include_attrs', [])
        exclude = metadata.get('exclude_attrs', [])
        replace = metadata.get('replace_attrs', {})
        picklers = metadata.get('picklers', {})

        cls = type(self)
        state = {'_class': '{}.{}'.format(cls.__module__, cls.__name__)}
        for attr in self:
            key = attr
            if key in replace:
                key = replace[key]
            if key.startswith('_') and key not in include and key != '_id':
                continue
            elif key in exclude:
                continue
            if attr in picklers:
                value = picklers[attr]['pickle'](self, self[attr])
            else:
                value = self._pickle(self[attr])
            if value is not None:
                state[key] = value
        return state

    def __setstate__(self, data, recurse=True):
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
                state[key] = self._unpickle(value)

        self.__dict__.update(state)

    def __str__(self):
        return '{}(\'{}\')'.format(self.__class__.__name__, self._id)
