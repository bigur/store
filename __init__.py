'''Поддержка хранения объектов в БД MongoDB.'''

__author__ = 'Gennady Kovalev <gik@bigur.ru>'
__copyright__ = '(c) 2016-2018 Business group for development management'
__licence__ = 'For license information see LICENSE'

from .database import DBProxy, db
from .stored import Stored
