'''Поддержка хранения объектов в БД MongoDB.'''

__author__ = 'Gennady Kovalev <gik@bigur.ru>'
__copyright__ = '(c) 2016-2018 Business group for development management'
__licence__ = 'For license information see LICENSE'

<<<<<<< HEAD:bigur/store/__init__.py
from .database import db
from .migrator import Migrator
from .stored import Stored
=======
from .database import DBProxy, db
from .stored import Stored, LazyRef
>>>>>>> c175721062bb6382a5965587892b5b2decc105af:__init__.py
