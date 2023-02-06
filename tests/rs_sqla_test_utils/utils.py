__author__ = 'haleemur'

import re

import sqlalchemy as sa
from packaging.version import Version
from sqlalchemy.engine.url import URL


def clean(query):
    query = re.sub(r'\s+ENCODE\s+\w+', '', query)
    query = re.sub(r'\s+CONSTRAINT\s+[a-zA-Z0-9_".]+', '', query)
    return re.sub(r'\s+', ' ', query).strip()


def compile_query(q, _dialect):
    return str(q.compile(
        dialect=_dialect,
        compile_kwargs={'literal_binds': True})
    )


def make_mock_engine(name):
    """
    Creates a mock sqlalchemy engine for testing dialect functionality

    """
    if Version(sa.__version__) >= Version('1.4.0'):
        return sa.create_mock_engine(URL(
            host='localhost',
            port=5432,
            username="",
            password="",
            database="example",
            drivername=name,
            query={'sslmode': 'prefer'}
        ), executor=None)

    return sa.create_engine(URL(
        drivername=name,
    ), strategy='mock', executor=None)
