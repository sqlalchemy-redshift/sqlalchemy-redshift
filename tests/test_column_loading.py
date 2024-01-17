from unittest import TestCase

from packaging.version import Version
import sqlalchemy as sa
from sqlalchemy.types import NullType, VARCHAR

from sqlalchemy_redshift.dialect import (
    RedshiftDialect_psycopg2, RedshiftDialect_psycopg2cffi,
    IcebergString, IcebergBinary
)

sa_version = Version(sa.__version__)


class TestColumnReflection(TestCase):
    def test_varchar_as_nulltype(self):
        """
        Varchar columns with no length should be considered NullType columns
        """
        for dialect in [
            RedshiftDialect_psycopg2(), RedshiftDialect_psycopg2cffi()
        ]:

            null_info = dialect._get_column_info(
                name='Null Column',
                format_type='character varying',
                default=None,
                notnull=False,
                domains={},
                enums=[],
                schema='default',
                encode='',
                comment='test column',
                identity=None
            )
            assert isinstance(null_info['type'], NullType)

            varchar_info = dialect._get_column_info(
                name='character column',
                format_type='character varying(30)',
                default=None,
                notnull=False,
                domains={},
                enums=[],
                schema='default',
                encode='',
                comment='test column',
                identity=None
            )
            assert isinstance(varchar_info['type'], VARCHAR)

            iceberg_string_info = dialect._get_column_info(
                name='Iceberg String Column',
                format_type='string',
                default=None,
                notnull=False,
                domains={},
                enums=[],
                schema='default',
                encode='',
                comment='test column',
                identity=None
            )
            assert isinstance(iceberg_string_info['type'], IcebergString)

            iceberg_binary_info = dialect._get_column_info(
                name='Iceberg Binary Column',
                format_type='binary',
                default=None,
                notnull=False,
                domains={},
                enums=[],
                schema='default',
                encode='',
                comment='test column',
                identity=None
            )
            assert isinstance(iceberg_binary_info['type'], IcebergBinary)
