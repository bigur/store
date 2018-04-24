#!/usr/bin/env python3

'''Модуль для тестирования хранения объектов.'''

__author__ = 'Gennady Kovalev <gik@bigur.ru>'
__copyright__ = '(c) 2016-2017 Business group for development management'
__licence__ = 'For license information see LICENSE'

import asyncio
import unittest

import office
import office.stored


class Person(office.stored.Stored):
    '''Люди.'''
    __metadata__ = {
        'collection': 'persons'
    }

    def __init__(self, name, address=None, order=None):
        self.name = name
        self.address = address
        self.order = order
        super().__init__()


class Man(Person):
    '''Мужчины.'''
    pass


class Woman(Person):
    '''Женщины.'''
    pass


class Address(office.stored.Stored):
    '''Адрес.'''

    def __init__(self, street, house, flat):
        self.street = street
        self.house = house
        self.flat = flat
        super().__init__()

    def full(self): # pylint: disable=missing-docstring
        return '{}, {}, {}'.format(self.street, self.house, self.flat)


class TestStored(unittest.TestCase):
    '''Тестирование хранения лобъектов в БД.'''

    def setUp(self):
        self.loop = asyncio.get_event_loop()

    def tearDown(self):
        self.loop.run_until_complete(office.World().db.client.drop_database('test'))

    def test_create_object(self):
        '''Stored: создание и сохранение объекта'''
        async def save(): #pylint: disable=missing-docstring
            man = Man(name='Иван')
            await man.save()
            newman = await Man.find_one({'_id': man.id})
            self.assertTrue(isinstance(newman, Man))
            self.assertEqual(newman.id, man.id)
        self.loop.run_until_complete(save())

    def test_edit_object(self):
        '''Stored: изменение объекта'''
        async def edit(): #pylint: disable=missing-docstring
            man = Man(name='Иван')
            await man.save()
            man.name = 'Пётр'
            await man.save()

            newman = await Man.find_one({'_id': man.id})
            self.assertEqual(newman.name, 'Пётр')
        self.loop.run_until_complete(edit())

    def test_lazy_ref(self):
        '''Stored: ленивая ссылка'''
        async def ref(): #pylint: disable=missing-docstring
            address = Address('Тверская ул.', 14, 5)
            await address.save()

            man = Man(name='Иван', address=address)
            await man.save()

            newman = await Man.find_one({'_id': man.id})
            self.assertTrue(isinstance(newman.address, office.stored.LazyRef))

            newman.address = await newman.address.resolve()
            self.assertEqual(newman.address.full(), 'Тверская ул., 14, 5')
        self.loop.run_until_complete(ref())

    def test_get_list(self):
        '''Stored: получение списка объектов'''
        async def get_list(): #pylint: disable=missing-docstring
            ivan = await Man(name='Иван', order=1).save()
            maria = await Woman(name='Марья', order=2).save()

            people = []
            async for person in Person.find().sort('order'):
                self.assertTrue(isinstance(person, Person))
                people.append(person)
            self.assertEqual([ivan.id, maria.id], [x.id for x in people])

        self.loop.run_until_complete(get_list())
