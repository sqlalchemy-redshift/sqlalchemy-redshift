import pytest
from rs_sqla_test_utils.utils import make_mock_engine
from sqlalchemy.dialects.postgresql import psycopg2, psycopg2cffi
from sqlalchemy.dialects.postgresql.base import PGDialect


@pytest.mark.parametrize(
    "name, expected_dialect",
    [
        ("redshift", psycopg2.dialect),
        ("redshift+psycopg2", psycopg2.dialect),
        ("redshift+psycopg2cffi", psycopg2cffi.dialect),
        ("redshift+redshift_connector", PGDialect),
    ],
)
def test_dialect_inherits_from_sqlalchemy_dialect(name, expected_dialect):
    engine = make_mock_engine(name)

    assert isinstance(engine.dialect, expected_dialect)

