__author__ = 'Gennady Kovalev <gik@bigur.ru>'
__copyright__ = '(c) 2016-2019 Business group for development management'
__licence__ = 'For license information see LICENSE'

from pytest import mark

from bigur.utils import config

from bigur.store.database import DBProxy


class TestDatabase(object):
    '''Тестирование кобъектов с базой данных'''
    @mark.asyncio
    async def test_proxy_cache(self):
        '''Тест кеширования прокси из параметров'''
        conf = config.get_object()
        conf.add_section('section1')
        conf.set('section1', 'url', 'mongodb://host1/db1')
        conf.add_section('section2')
        conf.set('section2', 'url', 'mongodb://host2/db2')
        db1 = DBProxy.get_for_config('section1', 'url')
        db2 = DBProxy.get_for_config('section2', 'url')
        db3 = DBProxy.get_for_config('section2', 'url')
        assert db1 != db2
        assert db2 == db3
