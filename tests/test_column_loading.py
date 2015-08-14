from unittest import TestCase

from sqlalchemy import types


def test_varchar_as_nulltype(redshift_dialect):
    """
    Varchar columns with no length should be considered NullType columns
    """
    column_info = redshift_dialect._get_column_info(
        'Null Column',
        'character varying', None, False, {}, {}, 'default'
    )
    assert isinstance(column_info['type'], types.NullType)


def test_varying_sometimes_not_nulltype(redshift_dialect):
    column_info_1 = redshift_dialect._get_column_info(
        'character column',
        'character varying(30)', None, False, {}, {}, 'default'
    )
    assert isinstance(column_info_1['type'], types.VARCHAR)
