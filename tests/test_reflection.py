import pytest
from sqlalchemy import MetaData, Table
from sqlalchemy.schema import CreateTable
from sqlalchemy.exc import NoSuchTableError

from sqlalchemy_redshift import dialect

from rs_sqla_test_utils import models, utils


def table_to_ddl(table):
    return str(CreateTable(table).compile(
        dialect=dialect.RedshiftDialect()
    ))


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
    (models.ReflectionSortKeyDistKeyWithSpaces, """
    CREATE TABLE sort_key_with_spaces (
        "col with spaces" INTEGER NOT NULL
    ) DISTSTYLE KEY DISTKEY ("col with spaces") SORTKEY ("col with spaces")
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
        col3 INTEGER,
        PRIMARY KEY (col1)
    ) DISTSTYLE EVEN
    """),
    (models.ReflectionDelimitedTableName, """
    CREATE TABLE other_schema."this.table" (
        id INTEGER NOT NULL,
        PRIMARY KEY (id)
    ) DISTSTYLE EVEN
    """),
    (models.ReflectionDelimitedTableNoSchema, """
    CREATE TABLE "this.table" (
        id INTEGER NOT NULL,
        PRIMARY KEY (id)
    ) DISTSTYLE EVEN
    """),
    (models.BasicInOtherSchema, """
    CREATE TABLE other_schema.basic (
        col1 INTEGER NOT NULL,
        PRIMARY KEY (col1)
    ) DISTSTYLE KEY DISTKEY (col1) SORTKEY (col1)
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
    (models.Referencing, '''
    CREATE TABLE other_schema.referencing (
        referenced_table_id INTEGER NOT NULL,
        PRIMARY KEY (referenced_table_id),
        FOREIGN KEY(referenced_table_id) REFERENCES
            other_schema.referenced (id)
    ) DISTSTYLE EVEN
    '''),
    (models.Referenced, '''
    CREATE TABLE other_schema.referenced (
        id INTEGER IDENTITY(1,1) NOT NULL,
        PRIMARY KEY (id)
    ) DISTSTYLE EVEN
    '''),
    (models.ReflectionCompositeForeignKeyConstraint, '''
    CREATE TABLE reflection_composite_fk_constraint (
        id INTEGER NOT NULL,
        col1 INTEGER,
        col2 INTEGER,
        PRIMARY KEY (id),
        FOREIGN KEY(col1, col2)
        REFERENCES reflection_pk_constraint (col1, col2)
    ) DISTSTYLE EVEN
    '''),
]


@pytest.mark.parametrize("model, ddl", models_and_ddls)
def test_definition(model, ddl):
    model_ddl = table_to_ddl(model.__table__)
    assert utils.clean(model_ddl) == utils.clean(ddl)


@pytest.mark.parametrize("model, ddl", models_and_ddls)
def test_reflection(redshift_session, model, ddl):
    metadata = MetaData(bind=redshift_session.bind)
    schema = model.__table__.schema
    table = Table(model.__tablename__, metadata,
                  schema=schema, autoload=True)
    introspected_ddl = table_to_ddl(table)
    assert utils.clean(introspected_ddl) == utils.clean(ddl)


def test_no_table_reflection(redshift_session):
    metadata = MetaData(bind=redshift_session.bind)
    with pytest.raises(NoSuchTableError):
        Table('foobar', metadata, autoload=True)


def test_no_search_path_leak(redshift_session):
    metadata = MetaData(bind=redshift_session.bind)
    Table('basic', metadata, autoload=True)
    result = redshift_session.execute("SHOW search_path")
    search_path = result.scalar()
    assert 'other_schema' not in search_path
