'''Тестирование документа.'''

__author__ = 'Gennady Kovalev <gik@bigur.ru>'
__copyright__ = '(c) 2016-2018 Business group for development management'
__licence__ = 'For license information see LICENSE'

# pylint: disable=unused-argument,redefined-outer-name,unused-import

from pytest import mark

from bigur.store import Embedded, Stored, LazyRef

from bigur.store.test.fixtures import configured, database, debug


class Person(Stored):
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


class Address(Stored):
    '''Адрес.'''

    def __init__(self, street, house, flat):
        self.street = street
        self.house = house
        self.flat = flat
        super().__init__()

    def full(self) -> str: # pylint: disable=missing-docstring
        return '{}, {}, {}'.format(self.street, self.house, self.flat)


class Sender(Embedded):
    '''Отправитель.'''
    def __init__(self, name: str, address: str) -> None:
        self.name = name
        self.address = address


class Letter(Stored):
    '''Письмо.'''
    def __init__(self, sender: Sender) -> None:
        self.sender = sender
        super().__init__()


class TestStored(object):
    '''Тестирование хранения объектов в БД.'''

    @configured
    @mark.asyncio
    async def test_create_object(self, database):
        '''Cоздание и сохранение объекта'''
        man = Man(name='Иван')
        await man.save()
        newman = await Man.find_one({'_id': man.id})
        assert isinstance(newman, Man)
        assert newman.id == man.id

    @configured
    @mark.asyncio
    async def test_edit_object(self, database, debug):
        '''Изменение объекта'''
        man = Man(name='Иван')
        await man.save()
        man.name = 'Пётр'
        await man.save()

        newman = await Man.find_one({'_id': man.id})
        assert newman.name == 'Пётр'

    @configured
    @mark.asyncio
    async def test_lazy_ref(self, database):
        '''Ленивая ссылка'''
        address = Address('Тверская ул.', 14, 5)
        await address.save()

        man = Man(name='Иван', address=address)
        await man.save()

        newman = await Man.find_one({'_id': man.id})
        assert isinstance(newman.address, LazyRef)

        newman.address = await newman.address.resolve()
        assert newman.address.full() == 'Тверская ул., 14, 5'

    @configured
    @mark.asyncio
    async def test_get_list(self, database):
        '''Получение списка объектов'''
        ivan = await Man(name='Иван', order=1).save()
        maria = await Woman(name='Марья', order=2).save()

        people = []
        async for person in Person.find({}).sort('order'):
            assert isinstance(person, Person)
            people.append(person)
        assert [ivan.id, maria.id] == [x.id for x in people]

    @configured
    @mark.asyncio
    async def test_save_embedded(self, database):
        '''Сохранение встроенного документа'''
        letter = await Letter(sender=Sender('Иван', 'Москва')).save()
        letter = await Letter.find_one({'_id': letter.id})
        assert isinstance(letter.sender, Embedded)
        assert letter.sender.name == 'Иван'
        assert letter.sender.address == 'Москва'
