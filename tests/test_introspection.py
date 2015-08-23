from sqlalchemy.schema import CreateTable
import sqlalchemy as sa

from rs_sqla_test_utils import models


unicode_ = type(u'')

ddl = """CREATE TABLE test_introspecting_unique_constraint (
\tcol1 INTEGER, 
\tcol2 INTEGER, 
UNIQUE (col1, col2)
)
"""  # noqa because trailing spaces


def table_to_ddl(table, engine):
    return unicode_(CreateTable(table)).compile(engine)


def test_introspecting_unique_constraint(redshift_session):
    assert table_to_ddl(
        table=models.IntrospectionUnique.__table__,
        engine=redshift_session,
    ) == ddl

    metadata = sa.MetaData(bind=redshift_session)
    introspected_table = sa.Table(
        'test_introspecting_unique_constraint', metadata, autoload=True
    )

    assert table_to_ddl(
        table=introspected_table,
        engine=redshift_session
    ) == ddl
