import pytest
from sqlalchemy import MetaData, Table
from sqlalchemy.schema import CreateTable
from sqlalchemy.exc import NoSuchTableError

from rs_sqla_test_utils import models


def cleaned(statement):
    stripped_lines = [line.strip() for line in statement.split('\n')]
    joined = '\n'.join([line for line in stripped_lines if line])
    return joined


def table_to_ddl(session, table):
    return str(CreateTable(table)
               .compile(session.bind))


models_and_ddls = [
    (models.ReflectionDistKey, """
    CREATE TABLE reflection_distkey (
        col1 INTEGER NOT NULL,
        col2 INTEGER,
        PRIMARY KEY (col1)
    ) DISTSTYLE KEY DISTKEY (col1)
    """),
    (models.ReflectionSortKey, """
    CREATE TABLE reflection_sortkey (
        col1 INTEGER NOT NULL,
        col2 INTEGER,
        PRIMARY KEY (col1)
    ) DISTSTYLE EVEN SORTKEY (col1, col2)
    """),
    (models.ReflectionInterleavedSortKey, """
    CREATE TABLE reflection_interleaved_sortkey (
        col1 INTEGER NOT NULL,
        col2 INTEGER,
        PRIMARY KEY (col1)
    ) DISTSTYLE EVEN INTERLEAVED SORTKEY (col1, col2)
    """),
    (models.ReflectionUniqueConstraint, """
    CREATE TABLE reflection_unique_constraint (
        col1 INTEGER NOT NULL,
        col2 INTEGER,
        PRIMARY KEY (col1),
        UNIQUE (col1, col2)
    ) DISTSTYLE EVEN
    """),
    (models.ReflectionPrimaryKeyConstraint, """
    CREATE TABLE reflection_pk_constraint (
        col1 INTEGER NOT NULL,
        col2 INTEGER NOT NULL,
        PRIMARY KEY (col1, col2)
    ) DISTSTYLE EVEN
    """),
    (models.ReflectionForeignKeyConstraint, """
    CREATE TABLE reflection_fk_constraint (
        col1 INTEGER NOT NULL,
        col2 INTEGER,
        PRIMARY KEY (col1),
        FOREIGN KEY(col1) REFERENCES reflection_unique_constraint (col1)
    ) DISTSTYLE EVEN
    """),
    (models.ReflectionDefaultValue, """
    CREATE TABLE reflection_default_value (
        col1 INTEGER NOT NULL,
        col2 INTEGER DEFAULT 5,
        PRIMARY KEY (col1)
    ) DISTSTYLE EVEN
    """),
    (models.ReflectionIdentity, """
    CREATE TABLE reflection_identity (
        col1 INTEGER NOT NULL,
        col2 INTEGER IDENTITY(1,3),
        PRIMARY KEY (col1)
    ) DISTSTYLE EVEN
    """),
    (models.BasicInSchema, """
    CREATE TABLE schema.basic (
        col INTEGER NOT NULL,
        PRIMARY KEY (col1)
    ) DISTSTYLE KEY DISTKEY (col1)
    """),
    pytest.mark.xfail((models.ReflectionDelimitedIdentifiers1, '''
    CREATE TABLE "group" (
        "this ""is it""" INTEGER NOT NULL,
        "and this also" INTEGER,
        PRIMARY KEY ("this ""is it""")
    ) DISTSTYLE EVEN
    ''')),
    pytest.mark.xfail((models.ReflectionDelimitedIdentifiers2, '''
    CREATE TABLE "column" (
            "excellent! & column" INTEGER NOT NULL,
            "most @exce.llent " INTEGER,
            PRIMARY KEY ("excellent! & column"),
    ) DISTSTYLE EVEN
    ''')),
]


@pytest.mark.parametrize("model, ddl", models_and_ddls)
def test_definition(redshift_session, model, ddl):
    model_ddl = table_to_ddl(redshift_session, model.__table__)
    assert cleaned(model_ddl) == cleaned(ddl)


@pytest.mark.parametrize("model, ddl", models_and_ddls)
def test_reflection(redshift_session, model, ddl):
    metadata = MetaData(bind=redshift_session.bind)
    table = Table(model.__tablename__, metadata, autoload=True)
    introspected_ddl = table_to_ddl(redshift_session, table)
    assert cleaned(introspected_ddl) == cleaned(ddl)


def test_no_table_reflection(redshift_session):
    metadata = MetaData(bind=redshift_session.bind)
    with pytest.raises(NoSuchTableError):
        Table('foobar', metadata, autoload=True)
