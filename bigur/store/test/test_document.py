'''Тестирование документа БД.'''

__author__ = 'Gennady Kovalev <gik@bigur.ru>'
__copyright__ = '(c) 2016-2018 Business group for development management'
__licence__ = 'For license information see LICENSE'

from typing import Optional

from pytest import fixture, mark
from bigur.store import Stored, Embedded, UnitOfWork


class Flat(Embedded):
    '''Квартира.'''
    def __init__(self, number: int) -> None:
        self.number: int = number
        super().__init__()


class House(Embedded):
    '''Дом.'''
    def __init__(self, number: int, flat: Optional[Flat] = None) -> None:
        self.number: int = number
        self.flat: Optional[Flat] = flat
        super().__init__()


class Address(Stored):
    '''Адрес.'''

    def __init__(self, street: str, house: Optional[House] = None) -> None:
        self.street: str = street
        self.house: Optional[House] = house
        super().__init__()


class TestDocument(object):
    '''Тестирование документа БД.'''
    @mark.asyncio
    async def test_pickle_1(self):
        '''Дамп простого объекта в документ БД.'''
        address = Address('Никольская')
        state = address.__getstate__()
        assert state == {
            '_class': 'store.test.test_document.Address',
            '_id': address.id,
            'street': 'Никольская'
        }

    @mark.asyncio
    async def test_pickle_2(self):
        '''Дамп объекта с вложенным документом.'''
        address = Address('Никольская', House(25))
        state = address.__getstate__()
        assert state == {
            '_class': 'store.test.test_document.Address',
            '_id': address.id,
            'street': 'Никольская',
            'house': {
                '_class': 'store.test.test_document.House',
                'number': 25
            }
        }

    @mark.asyncio
    async def test_pickle_3(self):
        '''Дамп объекта с вложенным документом 2-го уровня.'''
        address = Address('Никольская', House(25, Flat(8)))
        state = address.__getstate__()
        assert state == {
            '_class': 'store.test.test_document.Address',
            '_id': address.id,
            'street': 'Никольская',
            'house': {
                '_class': 'store.test.test_document.House',
                'number': 25,
                'flat': {
                    '_class': 'store.test.test_document.Flat',
                    'number': 8
                }
            }
        }
