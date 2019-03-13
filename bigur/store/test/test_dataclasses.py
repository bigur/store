__author__ = 'Gennady Kovalev <gik@bigur.ru>'
__copyright__ = '(c) 2016-2019 Business group for development management'
__licence__ = 'For license information see LICENSE'

from dataclasses import dataclass

from pytest import mark

from bigur.store import Stored, UnitOfWork


@dataclass
class Realty(Stored):
    number: str


@dataclass
class Land(Realty):
    square: int


class TestDataclasses(object):
    '''Test store dataclasses objects'''

    @mark.asyncio
    @mark.db_configured
    async def test_save(self, database):
        '''Save dataclasses object'''
        async with UnitOfWork():
            land = Land(number='123', square=1000)
            stored_id = land.id

        async with UnitOfWork():
            land = await Land.find_one({'_id': stored_id})
            assert isinstance(land, Land)
            assert land.id == stored_id
