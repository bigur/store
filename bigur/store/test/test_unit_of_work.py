'''Тестирование единицы работы.'''

__author__ = 'Gennady Kovalev <gik@bigur.ru>'
__copyright__ = '(c) 2016-2018 Business group for development management'
__licence__ = 'For license information see LICENSE'

from typing import Optional

from pytest import fixture, mark

from bigur.store.document import Embedded, Stored
from bigur.store.unit_of_work import UnitOfWork, context


# pylint: disable=protected-access

@fixture
def debug(caplog):
    '''Отладка тестов.'''
    from logging import DEBUG
    caplog.set_level(DEBUG, logger='bigur.store')


class Flat(Embedded):
    '''Квартира.'''
    def __init__(self, number: int) -> None:
        self.number: int = number
        super().__init__()

    def __repr__(self):
        return 'Flat({})'.format(id(self))


class House(Embedded):
    '''Дом.'''
    def __init__(self, number: int, flat: Optional[Flat] = None) -> None:
        self.number: int = number
        self.flat: Optional[Flat] = flat
        super().__init__()

    def __repr__(self):
        return 'House({})'.format(id(self))


class Address(Stored):
    '''Адрес.'''

    def __init__(self, street: str, house: Optional[House] = None) -> None:
        self.street: str = street
        self.house: Optional[House] = house
        super().__init__()

    def __repr__(self):
        return 'Address({})'.format(id(self))


class TestUnitOfWork(object):
    '''Тесты единицы работы.'''

    @mark.asyncio
    async def test_context(self):
        '''Изменение переменной в контексте.'''
        assert context.get() is None
        async with UnitOfWork() as uow:
            assert context.get() is uow

    @mark.asyncio
    async def test_new(self):
        '''Метка "новый" при создании.'''
        async with UnitOfWork() as uow:
            address = Address('Тверская')
            assert list(uow._new.keys()) == [address.id]

    @mark.asyncio
    async def test_modify_new(self):
        '''Отсутствие метки "грязный" при изменении нового объекта.'''
        async with UnitOfWork() as uow:
            address = Address('Тверская')
            assert list(uow._new.keys()) == [address.id]

            address.street = 'Никольская'

            assert list(uow._new.keys()) == [address.id]
            assert list(uow._dirty.keys()) == []

    @mark.asyncio
    async def test_modify(self):
        '''Метка "грязный" при изменении объекта.'''
        async with UnitOfWork() as uow:
            address = object.__new__(Address)
            address.__setstate__({
                '__unit_of_work': uow,
                '_id': 'test',
                'street': 'Тверская'
            })
            assert address.street == 'Тверская'

            address.street = 'Никольская'

            assert list(uow._new.keys()) == []
            assert list(uow._dirty.keys()) == [address.id]

    @mark.asyncio
    async def test_nested_1(self):
        '''Изменение вложенных на 1 уровень документов.'''
        async with UnitOfWork() as uow:
            house = House(5)
            address = Address('Тверская', house)
            uow._new = {}

            address.house.number = 6

            assert list(uow._new.keys()) == []
            assert list(uow._dirty.keys()) == [address.id]
            assert uow._dirty[address.id][0] == address
            assert list(uow._dirty[address.id][1]) == ['house.number']

    @mark.asyncio
    async def test_nested_2(self, debug):
        '''Добавление документа 2 уровня вложенности.'''
        async with UnitOfWork() as uow:
            house = House(5)
            address = Address('Тверская', house)
            uow._new = {}

            flat = Flat(25)
            address.house.flat = flat

            assert list(uow._new.keys()) == []
            assert list(uow._dirty.keys()) == [address.id]
            assert uow._dirty[address.id][0] == address
            assert list(uow._dirty[address.id][1]) == ['house.flat']

    @mark.asyncio
    async def test_nested_3(self, debug):
        '''Изменение вложенных на 2 уровня документов.'''
        async with UnitOfWork() as uow:
            flat = Flat(25)
            house = House(5, flat)
            address = Address('Тверская', house)
            uow._new = {}

            address.house.flat.number = 24

            assert list(uow._new.keys()) == []
            assert list(uow._dirty.keys()) == [address.id]
            assert uow._dirty[address.id][0] == address
            assert list(uow._dirty[address.id][1]) == ['house.flat.number']
