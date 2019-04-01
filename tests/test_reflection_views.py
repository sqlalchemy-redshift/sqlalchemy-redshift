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


def test_late_binding_view_reflection(redshift_engine):
    table_ddl = "CREATE TABLE my_table (col1 INTEGER, col2 INTEGER)"
    view_query = "SELECT my_table.col1, my_table.col2 FROM public.my_table"
    view_ddl = ("CREATE VIEW my_late_view AS "
                "%s WITH NO SCHEMA BINDING" % view_query)
    conn = redshift_engine.connect()
    conn.execute(table_ddl)
    conn.execute(view_ddl)
    insp = inspect(redshift_engine)
    view_definition = insp.get_view_definition('my_late_view')

    # For some reason, Redshift returns the entire DDL for late binding views.
    assert(clean(compile_query(view_definition)) == clean(view_ddl))
    view = Table('my_late_view', MetaData(),
                 autoload=True, autoload_with=redshift_engine)
    assert(len(view.columns) == 2)


def test_spectrum_reflection(redshift_engine):
    table_ddl = """create external table spectrum.sales(
        salesid integer,
        listid integer,
        sellerid integer,
        pricepaid decimal(8,2),
        saletime timestamp)
        row format delimited
        fields terminated by '\t'
        stored as textfile
        location 's3://awssampledbuswest2/tickit/spectrum/sales/'
        table properties ('numRows'='172000');
    """
    conn = redshift_engine.connect()
    conn.execute(table_ddl)
    insp = inspect(redshift_engine)
    table_definition = insp.get_columns('sales', schema='spectrum')

    assert 'sales_id' in table_definition.columns
    assert 'pricepaid' in table_definition.columns
