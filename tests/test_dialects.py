import pytest

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import (
    pg8000, psycopg2, psycopg2cffi, pypostgresql, zxjdbc,
)
from sqlalchemy.engine.url import URL

from redshift_sqlalchemy import dialect


@pytest.mark.parametrize('name, expected_dialect', [
    ('redshift', psycopg2.dialect),
    # ('redshift+pg8000', pg8000.dialect),
    ('redshift+psycopg2', psycopg2.dialect),
    ('redshift+psycopg2cffi', psycopg2cffi.dialect),
    # ('redshift+pypostgresql', pypostgresql.dialect),
    # ('redshift+zxjdbc', zxjdbc.dialect),
])
def test_dialect(name, expected_dialect):
    engine = sa.create_engine(URL(
        drivername=name,
    ))

    assert isinstance(engine.dialect, dialect.RedshiftDialectMixin)
    assert isinstance(engine.dialect, expected_dialect)
