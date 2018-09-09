'''Ссылка на объект базы данных.'''

__author__ = 'Gennady Kovalev <gik@bigur.ru>'
__copyright__ = '(c) 2016-2018 Business group for development management'
__licence__ = 'For license information see LICENSE'

from bigur.store.database import db


class IntegrityError(Exception):
    pass


class NotResolved(Exception):
    pass


class LazyRef(object):
    '''Ленивая ссылка. Автоматически подгружает объект из базы данных при
    обращении к нему.'''
    __own_keys__ = ('_dbref', '_obj')

    def __init__(self, dbref):
        self.dbref = dbref
        self.obj = None

    @property
    def collection(self):
        return self.dbref.collection

    @property
    def id(self):
        return self.dbref.id

    async def resolve(self):
        if self._obj is None:
            dbase = self._dbref.database
            if dbase is None:
                dbase = db
            collection = dbase[self._dbref.collection] # pylint: disable=E1136
            obj = await collection.find_one({'_id': self.dbref.id})
            if obj is None:
                raise IntegrityError('не смог подгрузить объект из коллекции {} с ИД {}'.format(
                    self._dbref.collection, self.dbref.id))
            self.obj = obj
        return self.obj

    def __getattr__(self, key):
        if self.obj is None:
            raise NotResolved('загрузите объект из базы с помощью .resolve()')
        return getattr(self._obj, key)

    def __setattr__(self, key, value):
        if key in self.__own_keys__:
            super().__setattr__(key, value)
        else:
            if self._obj is None:
                raise NotResolved('загрузите объект из базы с помощью <ref>.resolve()')
            setattr(self.obj, key, value)
