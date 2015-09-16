from sqlalchemy import MetaData, Table, inspect
from sqlalchemy.schema import CreateTable

from rs_sqla_test_utils.utils import clean, compile_query


def table_to_ddl(engine, table):
    return str(CreateTable(table)
               .compile(engine))


def test_view_reflection(redshift_engine):
    table_ddl = "CREATE TABLE my_table (col1 INTEGER, col2 INTEGER)"
    view_query = "SELECT my_table.col1, my_table.col2 FROM my_table"
    view_ddl = "CREATE VIEW my_view AS %s" % view_query
    conn = redshift_engine.connect()
    conn.execute(table_ddl)
    conn.execute(view_ddl)
    insp = inspect(redshift_engine)
    view_definition = insp.get_view_definition('my_view')
    assert(clean(compile_query(view_definition)) == clean(view_query))
    view = Table('my_view', MetaData(),
                 autoload=True, autoload_with=redshift_engine)
    assert(len(view.columns) == 2)
