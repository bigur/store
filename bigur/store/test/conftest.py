__author__ = 'Gennady Kovalev <gik@bigur.ru>'
__copyright__ = '(c) 2016-2018 Business group for development management'
__licence__ = 'For license information see LICENSE'

from os import environ
from pytest import fixture, mark

from bigur.store import db


@fixture
def debug(caplog):
    '''Turn debug on.'''
    from logging import getLogger, DEBUG
    caplog.set_level(DEBUG)
    return getLogger(__name__).debug


@fixture
async def database():
    '''Database connection.'''
    db.configure(environ.get('BIGUR_TEST_DB'))
    yield db


mark.db_configured = mark.skipif(
    environ.get('BIGUR_TEST_DB') is None,
    reason='Please define BIGUR_TEST_DB with test database uri')
