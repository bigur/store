'''Единица работы.'''

__author__ = 'Gennady Kovalev <gik@bigur.ru>'
__copyright__ = '(c) 2016-2018 Business group for development management'
__licence__ = 'For license information see LICENSE'

from logging import getLogger
from typing import Dict, Union, Tuple, Set
from contextvars import ContextVar, Token  # pylint: disable=E0401

from bigur.store.typing import Id, Document


logger = getLogger('bigur.store.unit_of_work')

context: ContextVar = ContextVar('uow', default=None)


class UnitOfWork(object):
    '''Единица работы. Определение логической транзакции БД.'''

    def __init__(self) -> None:
        self._token: Union[Token, None] = None

        self._new: Dict[Id, Document] = {}
        self._dirty: Dict[Id, Tuple[Document, Set[str]]] = {}
        self._removed: Dict[Id, Document] = {}

        super().__init__()

    # Управление очередями
    def register_new(self, document: Document) -> None:
        '''Ставит документ в очередь для создания в БД.'''
        id_ = document.id
        if not id_:
            raise ValueError('Документ должен содержать ИД.')
        if id_ in self._dirty:
            raise ValueError('Документ уже изменён и не сохранён.')
        if id_ in self._removed:
            raise ValueError('Документ помечен на удаление.')
        self._new[id_] = document

    def register_dirty(self, document: Document, keys: Set[str]) -> None:
        '''Ставит документ в очередь для обновления.'''
        id_ = document.id
        if not id_:
            raise ValueError('Документ должен содержать ИД.')
        if id_ in self._removed:
            raise ValueError('Документ помечен на удаление.')
        if id_ not in self._new:
            if id_ in self._dirty:
                self._dirty[id_][1].update(keys)
            else:
                self._dirty[id_] = (document, keys)

    def register_removed(self, document: Document) -> None:
        '''Ставит документ в очередь для удаления из БД.'''
        id_ = document.id
        if id_ in self._new:
            del self._new[id_]
        else:
            if id_ in self._dirty:
                del self._dirty[id_]
            if id_ not in self._removed:
                self._removed[id_] = document

    # Сохранение изменений в БД
    async def insert_new(self) -> None:
        '''Создаёт новые документы в базе данных.'''
        for document in self._new.values():
            await type(document).insert_one(document)

    async def update_dirty(self) -> None:
        '''Обновляет документы в базе данных.'''
        for document, keys in self._dirty.values():
            await type(document).update_one(document, keys)

    async def delete_removed(self) -> None:
        '''Удаляет документы из базы данных.'''
        for document in self._removed.values():
            await type(document).delete_one(document)

    # Управление транзакцией
    async def commit(self) -> None:
        '''Сохраняет все запланированные изменения в БД.'''
        await self.insert_new()
        await self.update_dirty()
        await self.delete_removed()

    async def rollback(self) -> None:
        '''Отменяет все изменения в текущей единице работы. Сами объекты
        при этом не изменяются.'''
        self._new = {}
        self._dirty = {}
        self._removed = {}

    # Поддержка контекста
    async def __aenter__(self):
        self._token = context.set(self)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        context.reset(self._token)
        self._token = None

        if exc_type is not None:
            await self.rollback()
        else:
            await self.commit()
