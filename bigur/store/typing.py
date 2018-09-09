'''Поддержка статический типизации модуля.'''

__author__ = 'Gennady Kovalev <gik@bigur.ru>'
__copyright__ = '(c) 2016-2018 Business group for development management'
__licence__ = 'For license information see LICENSE'

from datetime import datetime
from typing import Generic, TypeVar, NewType, Union, Any, Dict

from bigur.store import abc


T_co = TypeVar('T_co', covariant=True)


Id = NewType('Id', str)
Scalar = Union[str, int, float, bool, bytes, datetime]
DatabaseDict = Dict[str, Any]


class Document(Generic[T_co], abc.Document):
    '''Тип документа БД.'''
