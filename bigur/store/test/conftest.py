'''Фикстуры для тестов.'''

__author__ = 'Gennady Kovalev <gik@bigur.ru>'
__copyright__ = '(c) 2016-2018 Business group for development management'
__licence__ = 'For license information see LICENSE'

from os import environ
from pytest import fixture, mark

from bigur.utils import config
from bigur.store.database import db


@fixture
def debug(caplog):
    '''Отладка тестов.'''
    from logging import getLogger, DEBUG
    caplog.set_level(DEBUG)
    return getLogger(__name__).debug


@fixture
async def database():
    '''Доступ к базе данных.'''
    conf = config.get_object()
    if not conf.has_section('general'):
        conf.add_section('general')
    conf.set('general', 'database_url', environ.get('BIGUR_TEST_DB'))
    db._db = None
    for collection in await db.list_collection_names():
        await db.drop_collection(collection)
    yield db


mark.db_configured = mark.skipif(
    environ.get('BIGUR_TEST_DB') is None, reason='Не настроена база данных')
