from sqlalchemy_redshift.dialect import RedshiftDialect
import sqlalchemy as sa


def test_func_now():
    dialect = RedshiftDialect()
    s = sa.select([sa.func.NOW().label("time")])
    compiled = s.compile(dialect=dialect)
    assert str(compiled) == "SELECT SYSDATE AS time"


def test_insert_cte():
    dialect = RedshiftDialect()
    table = sa.Table(
        'test_table',
        sa.MetaData(),
        sa.Column('test_column', sa.String()))
    cte = sa.select([sa.text('1 as num')]).cte('test_cte')
    query = sa.select([sa.column('num')]).select_from(cte)
    insert = table.insert().from_select(['test_column'], query)
    compiled = insert.compile(dialect=dialect)
    assert str(compiled) == (
        "INSERT INTO test_table (test_column) WITH test_cte AS \n"
        "(SELECT 1 as num)\n"
        " SELECT num \n"
        "FROM test_cte")
