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
    from logging import DEBUG
    caplog.set_level(DEBUG, logger='bigur.store')


@fixture
def database():
    '''Доступ к базе данных.'''
    conf = config.get_object()
    conf.add_section('general')
    conf.set('general', 'database_uri', environ.get('BIGUR_TEST_DB'))
    return db


configured = mark.skipif(environ.get('BIGUR_TEST_DB') is None,
                         reason='Не настроена база данных')
