import pytest
import sqlalchemy_redshift.dialect
import sqlalchemy
from sqlalchemy.engine import reflection
from sqlalchemy import MetaData


def test_defined_types():
    # These types are the officially supported Redshift types as of 20191121
    # AWS Redshift Docs Reference:
    # https://docs.aws.amazon.com/redshift/latest/dg/c_Supported_data_types.html
    assert sqlalchemy_redshift.dialect.VARCHAR \
        is sqlalchemy.sql.sqltypes.VARCHAR
    assert sqlalchemy_redshift.dialect.NullType \
        is sqlalchemy.sql.sqltypes.NullType
    assert sqlalchemy_redshift.dialect.SMALLINT \
        is sqlalchemy.sql.sqltypes.SMALLINT
    assert sqlalchemy_redshift.dialect.INTEGER \
        is sqlalchemy.sql.sqltypes.INTEGER
    assert sqlalchemy_redshift.dialect.BIGINT \
        is sqlalchemy.sql.sqltypes.BIGINT
    assert sqlalchemy_redshift.dialect.DECIMAL \
        is sqlalchemy.sql.sqltypes.DECIMAL
    assert sqlalchemy_redshift.dialect.REAL \
        is sqlalchemy.sql.sqltypes.REAL
    assert sqlalchemy_redshift.dialect.CHAR \
        is sqlalchemy.sql.sqltypes.CHAR
    assert sqlalchemy_redshift.dialect.DATE \
        is sqlalchemy.sql.sqltypes.DATE
    assert sqlalchemy_redshift.dialect.TIMESTAMP \
        is sqlalchemy.sql.sqltypes.TIMESTAMP
    assert sqlalchemy_redshift.dialect.DOUBLE_PRECISION \
        is sqlalchemy.dialects.postgresql.DOUBLE_PRECISION

    assert sqlalchemy_redshift.dialect.GEOMETRY \
        is not sqlalchemy.sql.sqltypes.TEXT

    assert sqlalchemy_redshift.dialect.SUPER \
        is not sqlalchemy.sql.sqltypes.TEXT

    assert sqlalchemy_redshift.dialect.TIMESTAMPTZ \
        is not sqlalchemy.sql.sqltypes.TIMESTAMP

    assert sqlalchemy_redshift.dialect.TIMETZ \
        is not sqlalchemy.sql.sqltypes.TIME

    assert sqlalchemy_redshift.dialect.HLLSKETCH \
        is not sqlalchemy.sql.sqltypes.TEXT

custom_type_inheritance = [
    (
        sqlalchemy_redshift.dialect.GEOMETRY,
        sqlalchemy.sql.sqltypes.TEXT
    ),
    (
        sqlalchemy_redshift.dialect.SUPER,
        sqlalchemy.sql.sqltypes.TEXT
    ),
    (
        sqlalchemy_redshift.dialect.TIMESTAMP,
        sqlalchemy.sql.sqltypes.TIMESTAMP
    ),
    (
        sqlalchemy_redshift.dialect.TIMETZ,
        sqlalchemy.sql.sqltypes.TIME
    ),
    (
        sqlalchemy_redshift.dialect.HLLSKETCH,
        sqlalchemy.sql.sqltypes.TEXT
    ),
]


@pytest.mark.parametrize("custom_type, super_type", custom_type_inheritance)
def test_custom_types_extend_super_type(custom_type, super_type):
    custom_type_inst = custom_type()
    assert isinstance(custom_type_inst, super_type)


column_and_ddl = [
    (
        sqlalchemy_redshift.dialect.GEOMETRY,
        (
            u"\nCREATE TABLE t1 ("
            u"\n\tid INTEGER NOT NULL, "
            u"\n\tname VARCHAR, "
            u"\n\ttest_col GEOMETRY, "
            u"\n\tPRIMARY KEY (id)\n)\n\n"
        )
    ),
    (
        sqlalchemy_redshift.dialect.SUPER,
        (
            u"\nCREATE TABLE t1 ("
            u"\n\tid INTEGER NOT NULL, "
            u"\n\tname VARCHAR, "
            u"\n\ttest_col SUPER, "
            u"\n\tPRIMARY KEY (id)\n)\n\n"
        )
    ),
    (
        sqlalchemy_redshift.dialect.TIMESTAMPTZ,
        (
            u"\nCREATE TABLE t1 ("
            u"\n\tid INTEGER NOT NULL, "
            u"\n\tname VARCHAR, "
            u"\n\ttest_col TIMESTAMPTZ, "
            u"\n\tPRIMARY KEY (id)\n)\n\n"
        )
    ),
    (
        sqlalchemy_redshift.dialect.TIMETZ,
        (
            u"\nCREATE TABLE t1 ("
            u"\n\tid INTEGER NOT NULL, "
            u"\n\tname VARCHAR, "
            u"\n\ttest_col TIMETZ, "
            u"\n\tPRIMARY KEY (id)\n)\n\n"
        )
    ),
    (
        sqlalchemy_redshift.dialect.HLLSKETCH,
        (
            u"\nCREATE TABLE t1 ("
            u"\n\tid INTEGER NOT NULL, "
            u"\n\tname VARCHAR, "
            u"\n\ttest_col HLLSKETCH, "
            u"\n\tPRIMARY KEY (id)\n)\n\n"
        )
    ),
]


@pytest.mark.parametrize("custom_datatype, expected", column_and_ddl)
def test_custom_types_ddl_generation(
        custom_datatype, expected, stub_redshift_dialect
):
    compiler = sqlalchemy_redshift.dialect.RedshiftDDLCompiler(
        stub_redshift_dialect, None
    )
    table = sqlalchemy.Table(
        't1',
        sqlalchemy.MetaData(),
        sqlalchemy.Column('id', sqlalchemy.INTEGER, primary_key=True),
        sqlalchemy.Column('name', sqlalchemy.String),
        sqlalchemy.Column('test_col', custom_datatype)
    )

    create_table = sqlalchemy.schema.CreateTable(table)
    actual = compiler.process(create_table)
    assert expected == actual


redshift_specific_datatypes = [
    sqlalchemy_redshift.dialect.GEOMETRY,
    sqlalchemy_redshift.dialect.SUPER,
    sqlalchemy_redshift.dialect.TIMETZ,
    sqlalchemy_redshift.dialect.TIMESTAMPTZ,
    sqlalchemy_redshift.dialect.HLLSKETCH
]


@pytest.mark.parametrize("custom_datatype", redshift_specific_datatypes)
def test_custom_types_reflection_inspection(
        custom_datatype, redshift_engine
):
    metadata = MetaData(bind=redshift_engine)
    sqlalchemy.Table(
        't1',
        metadata,
        sqlalchemy.Column('id', sqlalchemy.INTEGER, primary_key=True),
        sqlalchemy.Column('name', sqlalchemy.String),
        sqlalchemy.Column('test_col', custom_datatype),
        schema='public'
    )
    metadata.create_all()
    inspect = reflection.Inspector.from_engine(redshift_engine)

    actual = inspect.get_columns(table_name='t1', schema='public')
    assert len(actual) == 3
    assert isinstance(actual[2]['type'], custom_datatype)


@pytest.mark.parametrize("custom_datatype", redshift_specific_datatypes)
def test_custom_type_compilation(custom_datatype):
    dt = custom_datatype()
    compiled_dt = dt.compile()
    assert compiled_dt == dt.__visit_name__
