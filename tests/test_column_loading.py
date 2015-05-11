from unittest import TestCase

from sqlalchemy.types import NullType, VARCHAR

from redshift_sqlalchemy.dialect import RedshiftDialect


class TestColumnReflection(TestCase):
    def test_varchar_as_nulltype(self):
        """
        Varchar columns with no length should be considered NullType columns
        """
        dialect = RedshiftDialect()
        column_info = dialect._get_column_info(
            'Null Column',
            'character varying', None, False, {}, {}, 'default'
        )
        assert isinstance(column_info['type'], NullType)
        column_info_1 = dialect._get_column_info(
            'character column',
            'character varying(30)', None, False, {}, {}, 'default'
        )
        assert isinstance(column_info_1['type'], VARCHAR)
