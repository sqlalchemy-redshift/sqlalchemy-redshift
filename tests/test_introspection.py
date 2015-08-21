from inspect import cleandoc

from sqlalchemy import MetaData, Table
from sqlalchemy.schema import CreateTable


def test_introspecting_unique_constraint(redshift_session):
    text = """
    CREATE TABLE test_introspecting_unique_constraint (
        col1 INTEGER,
        col2 INTEGER,
        UNIQUE (col1, col2)
    )
    """
    redshift_session.execute(text)
    redshift_session.commit()
    meta = MetaData()
    meta.bind = redshift_session
    t = Table('test_introspecting_unique_constraint',
              meta, autoload=True)
    assert cleandoc(CreateTable(t).compile(redshift_session)) == cleandoc(text)
