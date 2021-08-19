from unittest import TestCase

import sqlalchemy as sa
from packaging.version import Version
from sqlalchemy.types import VARCHAR, NullType

from sqlalchemy_redshift.dialect import RedshiftDialect

sa_version = Version(sa.__version__)


class TestColumnReflection(TestCase):
    def test_varchar_as_nulltype(self):
        """
        Varchar columns with no length should be considered NullType columns
        """
        dialect = RedshiftDialect()

        null_info = dialect._get_column_info(
            name="Null Column",
            format_type="character varying",
            default=None,
            notnull=False,
            domains={},
            enums=[],
            schema="default",
            encode="",
            comment="test column",
            identity=None,
        )
        assert isinstance(null_info["type"], NullType)

        varchar_info = dialect._get_column_info(
            name="character column",
            format_type="character varying(30)",
            default=None,
            notnull=False,
            domains={},
            enums=[],
            schema="default",
            encode="",
            comment="test column",
            identity=None,
        )
        assert isinstance(varchar_info["type"], VARCHAR)
