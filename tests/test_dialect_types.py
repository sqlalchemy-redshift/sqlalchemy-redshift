import sqlalchemy_redshift.dialect
import sqlalchemy


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

    assert sqlalchemy_redshift.dialect.TIMESTAMPTZ \
        is not sqlalchemy.sql.sqltypes.TIMESTAMP


def test_custom_type():
    timestamptz = sqlalchemy_redshift.dialect.TIMESTAMPTZ()
    assert isinstance(timestamptz, sqlalchemy.sql.sqltypes.TIMESTAMP)

    compiler = sqlalchemy_redshift.dialect.RedshiftDDLCompiler(
        sqlalchemy_redshift.dialect.RedshiftDialect(), None
    )
    table = sqlalchemy.Table(
        't1',
        sqlalchemy.MetaData(),
        sqlalchemy.Column('id', sqlalchemy.INTEGER, primary_key=True),
        sqlalchemy.Column('name', sqlalchemy.String),
        sqlalchemy.Column(
            'created_at', sqlalchemy_redshift.dialect.TIMESTAMPTZ
        )
    )

    create_table = sqlalchemy.schema.CreateTable(table)
    actual = compiler.process(create_table)
    expected = (
        u"\nCREATE TABLE t1 ("
        u"\n\tid INTEGER NOT NULL, "
        u"\n\tname VARCHAR, "
        u"\n\tcreated_at TIMESTAMPTZ, "
        u"\n\tPRIMARY KEY (id)\n)\n\n"
    )
    assert expected == actual
