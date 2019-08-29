'''Поддержка хранения объектов в БД MongoDB.'''

__author__ = 'Gennady Kovalev <gik@bigur.ru>'
__copyright__ = '(c) 2016-2018 Business group for development management'
__licence__ = 'For license information see LICENSE'

# flake8: noqa

from .database import db
from .document import EmbeddedList, EmbeddedDict, Embedded, Stored
from .lazy_ref import LazyRef
from .migrator import migrate, transition
from .unit_of_work import UnitOfWork
