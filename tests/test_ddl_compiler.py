import difflib

import pytest
from sqlalchemy import Table, Column, Integer, String, MetaData
from sqlalchemy.exc import CompileError
from sqlalchemy.schema import CreateTable

from sqlalchemy_redshift.dialect import RedshiftDDLCompiler, RedshiftDialect


class TestDDLCompiler(object):

    @pytest.fixture
    def compiler(self):
        compiler = RedshiftDDLCompiler(RedshiftDialect(), None)
        return compiler

    def _compare_strings(self, expected, actual):
        assert expected is not None, "Expected was None"
        assert actual is not None, "Actual was None"

        a = [(c, c.encode('hex')) if c is not None else None for c in expected]
        b = [(c, c.encode('hex')) if c is not None else None for c in actual]
        return u"-expected, +actual\n" + u"\n".join(difflib.ndiff(a, b))

    def test_create_table_simple(self, compiler):

        table = Table('t1',
                      MetaData(),
                      Column('id', Integer, primary_key=True),
                      Column('name', String))

        create_table = CreateTable(table)
        actual = compiler.process(create_table)
        expected = (
            u"\nCREATE TABLE t1 ("
            u"\n\tid INTEGER NOT NULL, "
            u"\n\tname VARCHAR, "
            u"\n\tPRIMARY KEY (id)\n)\n\n"
        )
        assert expected == actual, self._compare_strings(expected, actual)

    def test_create_table_with_identity(self, compiler):

        table = Table(
            't1',
            MetaData(),
            Column('id', Integer, primary_key=True, info={'identity': [1, 2]}),
            Column('name', String),
        )

        create_table = CreateTable(table)
        actual = compiler.process(create_table)
        expected = (
            u"\nCREATE TABLE t1 ("
            u"\n\tid INTEGER IDENTITY(1,2) NOT NULL, "
            u"\n\tname VARCHAR, "
            u"\n\tPRIMARY KEY (id)\n)\n\n"
        )
        assert expected == actual, self._compare_strings(expected, actual)

    def test_create_table_with_diststyle(self, compiler):

        table = Table('t1',
                      MetaData(),
                      Column('id', Integer, primary_key=True),
                      Column('name', String),
                      redshift_diststyle="EVEN")

        create_table = CreateTable(table)
        actual = compiler.process(create_table)
        expected = (
            u"\nCREATE TABLE t1 ("
            u"\n\tid INTEGER NOT NULL, "
            u"\n\tname VARCHAR, "
            u"\n\tPRIMARY KEY (id)\n) "
            u"DISTSTYLE EVEN\n\n"
        )
        assert expected == actual, self._compare_strings(expected, actual)

    def test_invalid_diststyle(self, compiler):

        table = Table(
            't1',
            MetaData(),
            Column('id', Integer, primary_key=True),
            Column('name', String),
            redshift_diststyle="NOTEVEN"
        )

        create_table = CreateTable(table)

        with pytest.raises(CompileError):
            compiler.process(create_table)

    def test_create_table_with_distkey(self, compiler):

        table = Table('t1',
                      MetaData(),
                      Column('id', Integer, primary_key=True),
                      Column('name', String),
                      redshift_distkey="id")

        create_table = CreateTable(table)
        actual = compiler.process(create_table)
        expected = (
            u"\nCREATE TABLE t1 ("
            u"\n\tid INTEGER NOT NULL, "
            u"\n\tname VARCHAR, "
            u"\n\tPRIMARY KEY (id)\n) "
            u"DISTKEY (id)\n\n"
        )
        assert expected == actual, self._compare_strings(expected, actual)

    def test_create_table_with_sortkey(self, compiler):

        table = Table('t1',
                      MetaData(),
                      Column('id', Integer, primary_key=True),
                      Column('name', String),
                      redshift_sortkey="id")

        create_table = CreateTable(table)
        actual = compiler.process(create_table)
        expected = (
            u"\nCREATE TABLE t1 ("
            u"\n\tid INTEGER NOT NULL, "
            u"\n\tname VARCHAR, "
            u"\n\tPRIMARY KEY (id)\n) "
            u"SORTKEY (id)\n\n"
        )
        assert expected == actual, self._compare_strings(expected, actual)

    def test_create_table_with_unicode_sortkey(self, compiler):
        table = Table('t1',
                      MetaData(),
                      Column('id', Integer, primary_key=True),
                      Column('name', String),
                      redshift_sortkey=u"id")

        create_table = CreateTable(table)
        actual = compiler.process(create_table)
        expected = (
            u"\nCREATE TABLE t1 ("
            u"\n\tid INTEGER NOT NULL, "
            u"\n\tname VARCHAR, "
            u"\n\tPRIMARY KEY (id)\n) "
            u"SORTKEY (id)\n\n"
        )
        assert expected == actual, self._compare_strings(expected, actual)

    def test_create_table_with_multiple_sortkeys(self, compiler):

        table = Table('t1',
                      MetaData(),
                      Column('id', Integer, primary_key=True),
                      Column('name', String),
                      redshift_sortkey=["id", "name"])

        create_table = CreateTable(table)
        actual = compiler.process(create_table)
        expected = (
            u"\nCREATE TABLE t1 ("
            u"\n\tid INTEGER NOT NULL, "
            u"\n\tname VARCHAR, "
            u"\n\tPRIMARY KEY (id)\n) "
            u"SORTKEY (id, name)\n\n"
        )
        assert expected == actual, self._compare_strings(expected, actual)

    def test_create_table_all_together(self, compiler):
        table = Table('t1',
                      MetaData(),
                      Column('id', Integer, primary_key=True),
                      Column('name', String),
                      redshift_diststyle="KEY",
                      redshift_distkey="id",
                      redshift_sortkey=["id", "name"])

        create_table = CreateTable(table)
        actual = compiler.process(create_table)
        expected = (
            u"\nCREATE TABLE t1 ("
            u"\n\tid INTEGER NOT NULL, "
            u"\n\tname VARCHAR, "
            u"\n\tPRIMARY KEY (id)\n) "
            u"DISTSTYLE KEY DISTKEY (id) SORTKEY (id, name)\n\n"
        )
        assert expected == actual, self._compare_strings(expected, actual)

    def test_create_column_with_sortkey(self, compiler):
        table = Table('t1',
                      MetaData(),
                      Column('id', Integer, primary_key=True,
                             info=dict(sortkey=True)),
                      Column('name', String)
                      )

        create_table = CreateTable(table)
        actual = compiler.process(create_table)
        expected = (
            u"\nCREATE TABLE t1 ("
            u"\n\tid INTEGER SORTKEY NOT NULL, "
            u"\n\tname VARCHAR, "
            u"\n\tPRIMARY KEY (id)\n)\n\n"
        )
        assert expected == actual, self._compare_strings(expected, actual)

    def test_create_column_with_distkey(self, compiler):
        table = Table('t1',
                      MetaData(),
                      Column('id', Integer, primary_key=True,
                             info=dict(distkey=True)),
                      Column('name', String)
                      )

        create_table = CreateTable(table)
        actual = compiler.process(create_table)
        expected = (
            u"\nCREATE TABLE t1 ("
            u"\n\tid INTEGER DISTKEY NOT NULL, "
            u"\n\tname VARCHAR, "
            u"\n\tPRIMARY KEY (id)\n)\n\n"
        )
        assert expected == actual, self._compare_strings(expected, actual)

    def test_create_column_with_encoding(self, compiler):
        table = Table('t1',
                      MetaData(),
                      Column('id', Integer, primary_key=True,
                             info=dict(encode="LZO")),
                      Column('name', String)
                      )

        create_table = CreateTable(table)
        actual = compiler.process(create_table)
        expected = (
            u"\nCREATE TABLE t1 ("
            u"\n\tid INTEGER ENCODE LZO NOT NULL, "
            u"\n\tname VARCHAR, "
            u"\n\tPRIMARY KEY (id)\n)\n\n"
        )
        assert expected == actual, self._compare_strings(expected, actual)
