import sqlalchemy as sa

from redshift_sqlalchemy import dialect
from rs_sqla_test_utils.utils import clean, compile_query


t1 = sa.Table('t1', sa.MetaData(),
              sa.Column('col1', sa.Integer(), primary_key=True),
              sa.Column('col2', sa.Integer()))


def test_basic_view():
    expected_result = """
    CREATE VIEW myview AS SELECT t1.col1, t1.col2 FROM t1
    """
    selectable = sa.sql.select([t1])
    view = sa.Table('myview', sa.MetaData())
    create_view = dialect.CreateView(view, selectable)
    assert clean(expected_result) == clean(compile_query(create_view))


def test_view_with_column_names():
    expected_result = """
    CREATE VIEW myview (col3, col4) AS SELECT t1.col1, t1.col2 FROM t1
    """
    selectable = sa.sql.select([t1])
    view = sa.Table('myview', sa.MetaData(),
                    sa.Column('col3', sa.Integer(), primary_key=True),
                    sa.Column('col4', sa.Integer()))
    create_view = dialect.CreateView(view, selectable)
    assert clean(expected_result) == clean(compile_query(create_view))


def test_view_with_delimited_identifiers():
    expected_result = """
    CREATE VIEW "my nice view!" ("col#1", "select") AS
      SELECT t1.col1, t1.col2 FROM t1
    """
    selectable = sa.sql.select([t1])
    view = sa.Table('my nice view!', sa.MetaData(),
                    sa.Column('col#1', sa.Integer(), primary_key=True),
                    sa.Column('select', sa.Integer()))
    create_view = dialect.CreateView(view, selectable)
    assert clean(expected_result) == clean(compile_query(create_view))
