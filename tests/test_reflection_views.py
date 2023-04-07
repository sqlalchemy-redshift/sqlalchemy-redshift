from sqlalchemy import MetaData, Table, inspect
from sqlalchemy.schema import CreateTable
import sqlalchemy as sa
from rs_sqla_test_utils.utils import clean, compile_query


def table_to_ddl(engine, table):
    return str(CreateTable(table)
               .compile(engine))


def test_view_reflection(redshift_engine):
    table_ddl = "CREATE TABLE my_table (col1 INTEGER, col2 INTEGER)"
    view_query = "SELECT my_table.col1, my_table.col2 FROM my_table"
    view_ddl = "CREATE VIEW my_view AS %s" % view_query

    with redshift_engine.connect() as conn:
        try:
            conn.execute(sa.text(table_ddl))
            conn.execute(sa.text(view_ddl))
            conn.execute(sa.text("COMMIT"))
            insp = inspect(redshift_engine)
            view_definition = insp.get_view_definition('my_view')
            assert clean(
                compile_query(view_definition, redshift_engine.dialect)
            ) == clean(view_query)
            view = Table('my_view', MetaData(),
                         autoload=True, autoload_with=redshift_engine)
            assert(len(view.columns) == 2)
        finally:
            conn.execute(sa.text('DROP TABLE IF EXISTS my_table CASCADE'))
            conn.execute(sa.text('DROP VIEW IF EXISTS my_view CASCADE'))
            conn.execute(sa.text("COMMIT"))


def test_late_binding_view_reflection(redshift_engine):
    table_ddl = "CREATE TABLE my_table (col1 INTEGER, col2 INTEGER)"
    view_query = "SELECT my_table.col1, my_table.col2 FROM public.my_table"
    view_ddl = ("CREATE VIEW my_late_view AS "
                "%s WITH NO SCHEMA BINDING" % view_query)

    with redshift_engine.connect() as conn:
        try:
            conn.execute(sa.text(table_ddl))
            conn.execute(sa.text(view_ddl))
            conn.execute(sa.text("COMMIT"))
            insp = inspect(redshift_engine)
            view_definition = insp.get_view_definition('my_late_view')

            # Redshift returns the entire DDL for late binding views.
            assert clean(
                compile_query(view_definition, redshift_engine.dialect)
            ) == clean(view_ddl)
            view = Table('my_late_view', MetaData(),
                         autoload=True, autoload_with=redshift_engine)
            assert(len(view.columns) == 2)
        finally:
            conn.execute(sa.text('DROP TABLE IF EXISTS my_table CASCADE'))
            conn.execute(sa.text('DROP VIEW IF EXISTS my_late_view CASCADE'))
            conn.execute(sa.text('COMMIT'))
