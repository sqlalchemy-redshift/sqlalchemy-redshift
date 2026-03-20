__author__ = 'haleemur'

import re

import sqlalchemy as sa
from sqlalchemy.engine import url as sa_url


def clean(query):
    query = re.sub(r'\s+ENCODE\s+\w+', '', query)
    query = re.sub(r'\s+CONSTRAINT\s+[a-zA-Z0-9_".]+', '', query)
    return re.sub(r'\s+', ' ', query).strip()


def compile_query(q, _dialect):
    return str(q.compile(
        dialect=_dialect,
        compile_kwargs={'literal_binds': True})
    )


def get_url_builder():
    # Changed in version 1.4: The URL object is now an immutable object.
    # To create a URL, use make_url() or URL.create()
    return sa_url.URL.create


def make_mock_engine(name):
    """
    Creates a mock sqlalchemy engine for testing dialect functionality

    """
    url_builder = get_url_builder()
    return sa.create_mock_engine(url_builder(
        drivername=name
    ), executor=None)
