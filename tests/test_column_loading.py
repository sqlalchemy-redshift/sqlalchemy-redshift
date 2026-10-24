from collections import namedtuple
from unittest import TestCase, mock

from packaging.version import Version
import sqlalchemy as sa
from sqlalchemy.types import INTEGER, VARCHAR, NullType

from sqlalchemy_redshift.dialect import (
    RedshiftDialect_psycopg2,
    RedshiftDialect_psycopg2cffi,
)

sa_version = Version(sa.__version__)

RedshiftColumn = namedtuple(
    "RedshiftColumn", "name format_type default notnull schema encode comment"
)


class TestColumnReflection(TestCase):
    def test_varchar_as_nulltype(self):
        """
        Varchar columns with no length should be considered NullType columns
        """
        for dialect in [RedshiftDialect_psycopg2(), RedshiftDialect_psycopg2cffi()]:

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

    def test_get_columns_reflects_types(self):
        """
        get_columns builds each column type through PGDialect._reflect_type,
        whose signature differs between SQLAlchemy releases
        """
        columns = [
            RedshiftColumn("id", "integer", None, True, "public", "az64", None),
            RedshiftColumn(
                "name", "character varying(256)", None, False, "public", "lzo", None
            ),
        ]
        for dialect in [RedshiftDialect_psycopg2(), RedshiftDialect_psycopg2cffi()]:
            with mock.patch.object(
                dialect, "_get_redshift_columns", return_value=columns
            ):
                reflected = dialect.get_columns(None, "example", schema="public")

            assert [c["name"] for c in reflected] == ["id", "name"]
            assert isinstance(reflected[0]["type"], INTEGER)
            assert reflected[0]["nullable"] is False
            assert isinstance(reflected[1]["type"], VARCHAR)
            assert reflected[1]["type"].length == 256
            assert reflected[1]["info"] == {"encode": "lzo"}
