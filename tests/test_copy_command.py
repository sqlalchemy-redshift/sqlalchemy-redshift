import pytest
import sqlalchemy as sa
from sqlalchemy import exc as sa_exc

from sqlalchemy_redshift import dialect
from sqlalchemy_redshift import commands
from rs_sqla_test_utils.utils import clean, compile_query

access_key_id = 'IO1IWSZL5YRFM3BEW256'
secret_access_key = 'A1Crw8=nJwEq+9SCgnwpYbqVSCnfB0cakn=lx4M1'
creds = (
    (
        'aws_access_key_id={access_key_id}'
        ';aws_secret_access_key={secret_access_key}'
    ).format(
        access_key_id=access_key_id,
        secret_access_key=secret_access_key
    )
)


tbl = sa.Table(
    't1', sa.MetaData(),
    sa.Column('col1', sa.Unicode()),
    sa.Column('col2', sa.Unicode()),
    schema='schema1'
)
tbl2 = sa.Table('t1', sa.MetaData())


def test_basic_copy_case():
    expected_result = """
    COPY schema1.t1 FROM 's3://mybucket/data/listing/'
    WITH CREDENTIALS AS '%s'
    DELIMITER AS ','
    BLANKSASNULL
    EMPTYASNULL
    IGNOREHEADER AS 0
    TRUNCATECOLUMNS
    REGION 'eu-west-3'
    """ % creds

    copy = dialect.CopyCommand(
        tbl,
        data_location='s3://mybucket/data/listing/',
        access_key_id=access_key_id,
        secret_access_key=secret_access_key,
        truncate_columns=True,
        delimiter=',',
        ignore_header=0,
        empty_as_null=True,
        blanks_as_null=True,
        region='eu-west-3',
    )
    assert clean(expected_result) == clean(compile_query(copy))


def test_iam_role():
    """Tests the use of iam role instead of access keys."""

    aws_account_id = '000123456789'
    iam_role_name = 'redshiftrole'
    creds = 'aws_iam_role=arn:aws:iam::{0}:role/{1}'.format(
        aws_account_id,
        iam_role_name,
    )

    expected_result = """
    COPY schema1.t1 FROM 's3://mybucket/data/listing/'
    WITH CREDENTIALS AS '{creds}'
    """.format(creds=creds)

    copy = dialect.CopyCommand(
        tbl,
        data_location='s3://mybucket/data/listing/',
        aws_account_id=aws_account_id,
        iam_role_name=iam_role_name,
    )
    assert clean(expected_result) == clean(compile_query(copy))


def test_format():
    expected_result = """
    COPY t1 FROM 's3://mybucket/data/listing/'
    WITH CREDENTIALS AS '%s'
    FORMAT AS JSON AS 'auto'
    DELIMITER AS ','
    BLANKSASNULL
    EMPTYASNULL
    IGNOREHEADER AS 0
    TRUNCATECOLUMNS
    """ % creds
    copy = dialect.CopyCommand(
        tbl2,
        data_location='s3://mybucket/data/listing/',
        access_key_id=access_key_id,
        secret_access_key=secret_access_key,
        format='JSON',
        truncate_columns=True,
        delimiter=',',
        ignore_header=0,
        empty_as_null=True,
        blanks_as_null=True,
    )
    assert clean(expected_result) == clean(compile_query(copy))


@pytest.mark.parametrize('format_type', (
    commands.Format.orc,
    commands.Format.parquet,
))
def test_format__columnar(format_type):
    expected_result = """
    COPY t1 FROM 's3://mybucket/data/listing/'
    WITH CREDENTIALS AS '%s'
    FORMAT AS %s
    """ % (creds, format_type.value.upper())
    copy = dialect.CopyCommand(
        tbl2,
        data_location='s3://mybucket/data/listing/',
        access_key_id=access_key_id,
        secret_access_key=secret_access_key,
        format=format_type,
    )
    assert clean(expected_result) == clean(compile_query(copy))


def test_invalid_format():
    t = sa.Table('t1', sa.MetaData(), schema='schema1')
    with pytest.raises(ValueError):
        dialect.CopyCommand(
            t,
            data_location='s3://bucket',
            access_key_id=access_key_id,
            secret_access_key=secret_access_key,
            format=';drop table bobby_tables;'
        )


def test_fixed_width_format_without_widths():
    copy = dialect.CopyCommand(
        tbl,
        format=commands.Format.fixed_width,
        data_location='s3://bucket',
        access_key_id=access_key_id,
        secret_access_key=secret_access_key
    )
    with pytest.raises(sa_exc.CompileError,
                       match=r"^'fixed_width' argument required.*$"):
        compile_query(copy)


def test_compression():
    expected_result = """
    COPY schema1.t1 FROM 's3://mybucket/data/listing/'
    WITH CREDENTIALS AS '%s'
    DELIMITER AS ',' LZOP
    BLANKSASNULL
    EMPTYASNULL
    IGNOREHEADER AS 0
    TRUNCATECOLUMNS
    """ % creds
    copy = dialect.CopyCommand(
        tbl,
        data_location='s3://mybucket/data/listing/',
        access_key_id=access_key_id,
        secret_access_key=secret_access_key,
        compression='LZOP',
        truncate_columns=True,
        delimiter=',',
        ignore_header=0,
        empty_as_null=True,
        blanks_as_null=True,
    )
    assert clean(expected_result) == clean(compile_query(copy))


def test_invalid_compression():
    with pytest.raises(ValueError):
        dialect.CopyCommand(
            tbl,
            data_location='s3://bucket/of/joy',
            access_key_id=access_key_id,
            secret_access_key=secret_access_key,
            compression=';drop table bobby_tables;',
        )


def test_ascii_nul_as_redshift_null():
    expected_result = """
    COPY schema1.t1 FROM 's3://mybucket/data/listing/'
    WITH CREDENTIALS AS '%s'
    DELIMITER AS ','
    BZIP2
    BLANKSASNULL
    EMPTYASNULL
    IGNOREHEADER AS 0
    NULL AS '\0'
    TRUNCATECOLUMNS
    """ % creds
    copy = dialect.CopyCommand(
        tbl,
        data_location='s3://mybucket/data/listing/',
        access_key_id=access_key_id,
        secret_access_key=secret_access_key,
        compression='BZIP2',
        dangerous_null_delimiter=u'\000',
        truncate_columns=True,
        delimiter=',',
        ignore_header=0,
        empty_as_null=True,
        blanks_as_null=True,
    )
    assert clean(expected_result) == clean(compile_query(copy))


def test_json_upload_with_manifest_ordered_columns():
    expected_result = """
    COPY schema1.t1 (col1, col2) FROM 's3://mybucket/data/listing.manifest'
    WITH CREDENTIALS AS '%s'
    FORMAT AS JSON AS 's3://mybucket/data/jsonpath.json'
    GZIP
    MANIFEST
    ACCEPTANYDATE
    TIMEFORMAT AS 'auto'
    """ % creds
    copy = dialect.CopyCommand(
        [tbl.c.col1, tbl.c.col2],
        data_location='s3://mybucket/data/listing.manifest',
        access_key_id=access_key_id,
        secret_access_key=secret_access_key,
        manifest=True,
        format='JSON',
        path_file='s3://mybucket/data/jsonpath.json',
        compression='GZIP',
        time_format='auto',
        accept_any_date=True,
    )
    assert clean(expected_result) == clean(compile_query(copy))


def test_stat_update_maxerror_and_escape():
    expected_result = """
    COPY schema1.t1 FROM 's3://mybucket/data/listing/'
    WITH CREDENTIALS AS '%s'
    ESCAPE
    NULL AS '\x00'
    MAXERROR AS 0
    STATUPDATE ON
    """ % creds
    copy = dialect.CopyCommand(
        tbl,
        data_location='s3://mybucket/data/listing/',
        access_key_id=access_key_id,
        secret_access_key=secret_access_key,
        max_error=0,
        dangerous_null_delimiter=u'\000',
        stat_update=True,
        escape=True,
    )
    assert clean(expected_result) == clean(compile_query(copy))


def test_different_tables():
    metdata = sa.MetaData()
    t1 = sa.Table('t1', metdata, sa.Column('col1', sa.Unicode()))
    t2 = sa.Table('t2', metdata, sa.Column('col1', sa.Unicode()))
    with pytest.raises(ValueError):
        dialect.CopyCommand(
            [t1.c.col1, t2.c.col1],
            data_location='s3://bucket',
            access_key_id=access_key_id,
            secret_access_key=secret_access_key,
            format=dialect.Format.csv,
        )


def test_legacy_string_format():
    metdata = sa.MetaData()
    t1 = sa.Table('t1', metdata, sa.Column('col1', sa.Unicode()))
    t2 = sa.Table('t2', metdata, sa.Column('col1', sa.Unicode()))
    with pytest.raises(ValueError):
        dialect.CopyCommand(
            [t1.c.col1, t2.c.col1],
            data_location='s3://bucket',
            access_key_id=access_key_id,
            secret_access_key=secret_access_key,
            format='CSV',
        )
