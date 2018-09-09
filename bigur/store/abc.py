'''Абстрактные классы основных сущностей.'''

__author__ = 'Gennady Kovalev <gik@bigur.ru>'
__copyright__ = '(c) 2016-2018 Business group for development management'
__licence__ = 'For license information see LICENSE'


from abc import ABCMeta


class MetadataType(ABCMeta):
    '''Объединяет атрибуты __metadata__ с родительскими классами.'''

    def __init__(cls, name, bases, attrs):
        metadata = {}
        for class_ in reversed(cls.__mro__):
            meta = getattr(class_, '__metadata__', {})
            for key, value in meta.items():
                if key == 'replace_attrs':
                    if 'replace_attrs' not in metadata:
                        metadata['replace_attrs'] = {}
                    metadata['replace_attrs'].update(value)
                elif key == 'picklers':
                    if 'picklers' not in metadata:
                        metadata['picklers'] = {}
                    metadata['picklers'].update(value)
                else:
                    metadata[key] = value
        cls.__metadata__ = metadata

        super().__init__(name, bases, attrs)


class Document(metaclass=MetadataType):
    '''Абстрактный документ БД.'''
