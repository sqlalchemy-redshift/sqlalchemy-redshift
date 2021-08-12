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
    try:
        conn.execute(table_ddl)
        conn.execute(view_ddl)
        insp = inspect(redshift_engine)
        view_definition = insp.get_view_definition('my_view')
        assert(clean(compile_query(view_definition, redshift_engine.dialect)) == clean(view_query))
        view = Table('my_view', MetaData(),
                     autoload=True, autoload_with=redshift_engine)
        assert(len(view.columns) == 2)
    finally:
        conn.execute('DROP TABLE IF EXISTS my_table CASCADE')
        conn.execute('DROP VIEW IF EXISTS my_view CASCADE')


def test_late_binding_view_reflection(redshift_engine):
    table_ddl = "CREATE TABLE my_table (col1 INTEGER, col2 INTEGER)"
    view_query = "SELECT my_table.col1, my_table.col2 FROM public.my_table"
    view_ddl = ("CREATE VIEW my_late_view AS "
                "%s WITH NO SCHEMA BINDING" % view_query)
    conn = redshift_engine.connect()
    try:
        conn.execute(table_ddl)
        conn.execute(view_ddl)
        insp = inspect(redshift_engine)
        view_definition = insp.get_view_definition('my_late_view')

        # Redshift returns the entire DDL for late binding views.
        assert(clean(compile_query(view_definition, redshift_engine.dialect)) == clean(view_ddl))
        view = Table('my_late_view', MetaData(),
                     autoload=True, autoload_with=redshift_engine)
        assert(len(view.columns) == 2)
    finally:
        conn.execute('DROP TABLE IF EXISTS my_table CASCADE')
        conn.execute('DROP VIEW IF EXISTS my_late_view CASCADE')
