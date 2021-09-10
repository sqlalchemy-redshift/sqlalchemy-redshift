import pytest

from sqlalchemy.dialects.postgresql import (
    psycopg2, psycopg2cffi
)
from sqlalchemy.dialects.postgresql.base import PGDialect

from redshift_sqlalchemy import dialect
from tests.conftest import make_mock_engine


@pytest.mark.parametrize('name, expected_dialect', [
    ('redshift', psycopg2.dialect),
    ('redshift+psycopg2', psycopg2.dialect),
    ('redshift+psycopg2cffi', psycopg2cffi.dialect),
    ('redshift+redshift_connector', PGDialect),
])
def test_dialect_inherits_from_sqlalchemy_dialect(name, expected_dialect):
    engine = make_mock_engine(name)

    assert isinstance(engine.dialect, expected_dialect)


@pytest.mark.parametrize('name, expected_dialect', [
    ('redshift', dialect.Psycopg2RedshiftDialectMixin),
    ('redshift+psycopg2', dialect.Psycopg2RedshiftDialectMixin),
    ('redshift+psycopg2cffi', dialect.Psycopg2RedshiftDialectMixin),
])
def test_dialect_inherits_from_redshift_mixin(name, expected_dialect):
    engine = make_mock_engine(name)

    assert isinstance(engine.dialect, expected_dialect)


@pytest.mark.parametrize('name, expected_dialect', [
    ('redshift', dialect.RedshiftDialect_psycopg2),
    ('redshift+psycopg2', dialect.RedshiftDialect_psycopg2),
    ('redshift+psycopg2cffi', dialect.RedshiftDialect_psycopg2cffi),
])
def test_dialect_registered_correct_class(name, expected_dialect):
    engine = make_mock_engine(name)

    assert isinstance(engine.dialect, expected_dialect)


def test_redshift_dialect_synonym_of_redshift_dialect_psycopg2():
    assert isinstance(
        dialect.RedshiftDialect(),
        dialect.RedshiftDialect_psycopg2
    )
