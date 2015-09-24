import pytest
import sqlalchemy as sa

from redshift_sqlalchemy import dialect
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


tbl = sa.Table('t1', sa.MetaData(), schema='schema1')
tbl2 = sa.Table('t1', sa.MetaData())


def test_basic_copy_case():
    expected_result = """
    COPY schema1.t1 FROM 's3://mybucket/data/listing/'
    WITH CREDENTIALS AS '%s'
    FORMAT AS CSV
    DELIMITER AS ','
    BLANKSASNULL
    EMPTYASNULL
    IGNOREHEADER AS 0
    TRUNCATECOLUMNS
    """ % creds

    copy = dialect.CopyCommand(
        table=tbl,
        data_location='s3://mybucket/data/listing/',
        access_key_id=access_key_id,
        secret_access_key=secret_access_key,
        truncate_columns=True,
        delimiter=',',
        ignore_header=0,
        empty_as_null=True,
        blanks_as_null=True,
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
        table=tbl2,
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


def test_invalid_format():
    t = sa.Table('t1', sa.MetaData(), schema='schema1')
    with pytest.raises(ValueError):
        dialect.CopyCommand(
            table=t,
            data_location='s3://bucket',
            access_key_id=access_key_id,
            secret_access_key=secret_access_key,
            format=';drop table bobby_tables;'
        )


def test_compression():
    expected_result = """
    COPY schema1.t1 FROM 's3://mybucket/data/listing/'
    WITH CREDENTIALS AS '%s'
    FORMAT AS CSV DELIMITER AS ',' LZOP
    BLANKSASNULL
    EMPTYASNULL
    IGNOREHEADER AS 0
    TRUNCATECOLUMNS
    """ % creds
    copy = dialect.CopyCommand(
        table=tbl,
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
            table=tbl,
            data_location='s3://bucket/of/joy',
            access_key_id=access_key_id,
            secret_access_key=secret_access_key,
            compression=';drop table bobby_tables;',
        )


def test_ascii_nul_as_redshift_null():
    expected_result = """
    COPY schema1.t1 FROM 's3://mybucket/data/listing/'
    WITH CREDENTIALS AS '%s'
    FORMAT AS CSV
    DELIMITER AS ','
    LZOP
    BLANKSASNULL
    EMPTYASNULL
    IGNOREHEADER AS 0
    NULL AS'\0'
    TRUNCATECOLUMNS
    """ % creds
    copy = dialect.CopyCommand(
        table=tbl,
        data_location='s3://mybucket/data/listing/',
        access_key_id=access_key_id,
        secret_access_key=secret_access_key,
        compression='LZOP',
        dangerous_null_delimiter=u'\000',
        truncate_columns=True,
        delimiter=',',
        ignore_header=0,
        empty_as_null=True,
        blanks_as_null=True,
    )
    assert clean(expected_result) == clean(compile_query(copy))


def test_json_upload():
    expected_result = """
    COPY schema1.t1 FROM 's3://mybucket/data/listing/'
    WITH CREDENTIALS AS '%s'
    FORMAT AS JSON AS 'auto'
    GZIP
    ACCEPTANYDATE
    TIMEFORMAT AS 'auto'
    """ % creds
    copy = dialect.CopyCommand(
        table=tbl,
        data_location='s3://mybucket/data/listing/',
        access_key_id=access_key_id,
        secret_access_key=secret_access_key,
        format='JSON',
        compression='GZIP',
        time_format='auto',
        accept_any_date=True,
    )
    assert clean(expected_result) == clean(compile_query(copy))
