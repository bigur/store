'''Миграция базы данных.'''

__author__ = 'Gennady Kovalev <gik@bigur.ru>'
__copyright__ = '(c) 2016-2018 Business group for development management'
__licence__ = 'For license information see LICENSE'

from datetime import datetime
from logging import getLogger
from re import sub
from typing import Callable, Dict, List
from uuid import uuid4

from bigur.rx import ObserverBase

logger = getLogger(__name__)


class transition(object):
    '''Декаратор, определяет поведение метода миграции, с какой
    версии на какую он обновляет базу данных.'''

    def __init__(self, component, from_version, to_version):
        self.component = component
        self.from_version = from_version
        self.to_version = to_version

    def __call__(self, meth):
        Migrator.transitions.append(meth)
        meth.__transition__ = self
        return meth


class Migrator(ObserverBase):
    '''Мигратор, обеспечивает обновление базы данных с одной
    версии на другую.'''

    migrators: Dict[str, 'Migrator'] = {}
    transitions: List[Callable] = []

    def __init__(self, component, version):
        self.component = component
        self.version = version

        self.migrators[component] = self

        super().__init__()

    @staticmethod
    def normalize_version(version):
        '''Разбирает строку с номером версии и возвращает :class:`list` с
        тремя числами для сравнения.

        :param str version: строка с версией'''
        return [int(x) for x in sub(r'(\.0+)*$', '', version).split('.')]

    @classmethod
    def compare_versions(cls, left, right):
        '''Сравнивает номера версий. Если обе версии равны,
        то возвращается None, иначе возвращается ``left > right``.

        :param str left: строка с версией
        :param str right: строка с версией'''
        if left is None and right is None:
            return
        elif left is None:
            return False
        elif right is None:
            return True

        left = cls.normalize_version(left)
        right = cls.normalize_version(right)

        if left == right:
            return

        return left > right

    def get_transitions(self):
        '''Собирает методы миграции.'''
        transitions = {}
        for meth in self.transitions:
            trans = getattr(meth, '__transition__', None)
            if trans and trans.component == self.component:
                if trans.from_version not in transitions:
                    transitions[trans.from_version] = {}
                transitions[trans.from_version][trans.to_version] = meth
        return transitions

    async def on_next(self, db):
        '''Запуск миграции через AsyncObservable.'''
        logger.debug('Начинаю миграцию для %s', self.component)

        component_transitions = self.get_transitions()

        path = []
        db_version = await db.versions.find_one({'component': self.component})
        if db_version is not None:
            from_version = db_version['version']
        else:
            from_version = None

        while self.compare_versions(self.version, from_version):
            if from_version not in component_transitions:
                break

            legs = component_transitions[from_version]

            shortest = (None, None)
            for to_version, meth in legs.items():
                if shortest[0] is None:
                    shortest = to_version, meth
                else:
                    if self.compare_versions(to_version, shortest[0]):
                        shortest = to_version, meth

            if shortest[0] is None:
                break

            path.append(shortest)
            from_version = shortest[0]

        # Проверяем, не апгрейдит ли последний метод до версии большоей, чем
        # версия компоненты.
        if path and path[-1][0] != self.version:
            raise KeyError('последний мигратор апгрейдит компоненту до '
                           'версии, большей чем сама компонента')

        for to_version, meth in path:
            logger.debug('Обновляю БД до версии %s для компоненты %s',
                         to_version, self.component)

            orig_db = db.client.get_database(db.name)
            await meth(orig_db)

            if db_version is None:
                db_version = {
                    '_id': str(uuid4()),
                    'component': self.component,
                    'version': to_version,
                    'timestamp': datetime.utcnow()
                }
                await orig_db.versions.insert_one(db_version)
            else:
                await orig_db.versions.update_one({
                    '_id': db_version['_id']
                }, {
                    '$set': {
                        'timestamp': datetime.utcnow(),
                        'version': to_version
                    }
                })

    async def on_error(self, error: Exception):
        raise error

    async def on_completed(self):
        logger.debug('Закончил миграцию для %s', self.component)
