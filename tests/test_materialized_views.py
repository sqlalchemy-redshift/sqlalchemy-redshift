import pytest

from sqlalchemy import Table, Integer, String, MetaData, Column, select
from sqlalchemy_redshift import ddl
from rs_sqla_test_utils.utils import clean, compile_query

@pytest.fixture
def selectable():
    table = Table('t1',
                  MetaData(),
                  Column('id', Integer, primary_key=True),
                  Column('name', String))
    return select([table.c.id, table.c.name], from_obj=table)


def test_basic_mv_case(selectable):
    expected_result = """
    CREATE MATERIALIZED VIEW test_view
    AS SELECT t1.id, t1.name FROM t1
    """
    copy = ddl.CreateMaterializedView(
        "test_view",
        selectable
    )
    assert clean(expected_result) == clean(compile_query(copy))

def test_no_backup_mv_case(selectable):
    expected_result = """
    CREATE MATERIALIZED VIEW test_view
    BACKUP NO
    AS SELECT t1.id, t1.name FROM t1
    """
    copy = ddl.CreateMaterializedView(
        "test_view",
        selectable,
        backup=False
    )
    assert clean(expected_result) == clean(compile_query(copy))
