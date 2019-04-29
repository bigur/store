#!/usr/bin/env python3

__author__ = 'Gennady Kovalev <gik@bigur.ru>'
__copyright__ = '(c) 2016-2019 Business group for development management'
__licence__ = 'For license information see LICENSE'

from setuptools import setup

exec(  # pylint: disable=W0122
    compile(
        open('bigur/store/version.py', "rb").read(), 'bigur/store/version.py',
        'exec'), globals(), locals())

setup(
    name='bigur-store',
    # pylint: disable=E0602
    version=__version__,  # type: ignore  # noqa
    description='MongoDB support for bigur system',
    url='https://github.com/bigur/store',
    author='Gennady Kovalev',
    author_email='gik@bigur.ru',
    license='BSD-3-Clause',
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Programming Language :: Python :: 3.5',
    ],
    keywords=['bigur', 'store', 'mongodb'],
    packages=['bigur/store'])
