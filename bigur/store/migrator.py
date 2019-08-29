__author__ = 'Gennady Kovalev <gik@bigur.ru>'
__copyright__ = '(c) 2016-2019 Development management business group'
__licence__ = 'For license information see LICENSE'

from datetime import datetime
from logging import getLogger
from re import sub
from typing import Callable, Dict, List, Optional, Tuple

from pymongo.database import Database

logger = getLogger(__name__)

migrators: Dict[str, Dict[str, Tuple[str, Callable]]] = {}


def transition(component: str, from_version: str, to_version: str
               ) -> Callable[[Callable[[Database], None]], None]:
    def wrap(func):
        if component not in migrators:
            migrators[component] = {}
        if migrators[component].get(from_version) is None:
            migrators[component][from_version] = (to_version, func)
        else:
            raise ValueError('Migrator function from version '
                             '{} already exists'.format(from_version))

    return wrap


def normalize_version(version: str) -> List[int]:
    return [int(x) for x in sub(r'(\.0+)*$', '', version).split('.')]


def compare_versions(left: str, right: str) -> Optional[bool]:
    if left is None and right is None:
        return None
    elif left is None:
        return False
    elif right is None:
        return True

    normal_left = normalize_version(left)
    normal_right = normalize_version(right)

    if normal_left == normal_right:
        return None

    return normal_left > normal_right


def migrate(db: Database, component: str, version: str):
    logger.debug('Migration for %s to version %s started', component, version)

    db_version = db.versions.find_one({'component': component})
    if db_version is not None:
        from_version = db_version['version']
    else:
        from_version = None

    logger.debug('Database version is %s', from_version)

    transitions = migrators.get(component, {})

    path: List[Tuple[str, Callable[[Database], None]]] = []

    while True:
        if from_version in transitions:
            to_version, func = transitions[from_version]
            logger.debug('Append to path %s -> %s: %s', from_version,
                         to_version, func)
            path.append((to_version, func))
            from_version = to_version
            if from_version == version:
                break
        else:
            break

    for to_version, func in path:
        logger.info('Updating database to version %s for %s component',
                    to_version, component)

        func(db)

        if db_version is None:
            db_version = {
                'component': component,
                'version': to_version,
                'timestamp': datetime.utcnow()
            }
            db.versions.insert_one(db_version)
        else:
            db.versions.update_one({
                '_id': db_version['_id']
            }, {
                '$set': {
                    'timestamp': datetime.utcnow(),
                    'version': to_version
                }
            })
