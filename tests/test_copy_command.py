import pytest

import re

import sqlalchemy as sa

from redshift_sqlalchemy import dialect


def clean(query):
    return re.sub(r'\s+', ' ', query).strip()


def compile_query(q):
    return str(q.compile(
        dialect=dialect.RedshiftDialect(),
        compile_kwargs={'literal_binds': True})
    )

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
    CREDENTIALS '%s'
    CSV TRUNCATECOLUMNS DELIMITER ',' IGNOREHEADER 0 EMPTYASNULL BLANKSASNULL
    """ % creds

    copy = dialect.CopyCommand(
        table=tbl,
        data_location='s3://mybucket/data/listing/',
        access_key_id=access_key_id,
        secret_access_key=secret_access_key,
    )
    assert clean(expected_result) == clean(compile_query(copy))


def test_format():
    expected_result = """
    COPY t1 FROM 's3://mybucket/data/listing/'
    CREDENTIALS '%s'
    JSON TRUNCATECOLUMNS DELIMITER ',' IGNOREHEADER 0 EMPTYASNULL BLANKSASNULL
    """ % creds
    copy = dialect.CopyCommand(
        table=tbl2,
        data_location='s3://mybucket/data/listing/',
        access_key_id=access_key_id,
        secret_access_key=secret_access_key,
        format='JSON',
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
    CREDENTIALS '%s'
    CSV TRUNCATECOLUMNS DELIMITER ',' IGNOREHEADER 0 LZOP
    EMPTYASNULL BLANKSASNULL
    """ % creds
    copy = dialect.CopyCommand(
        table=tbl,
        data_location='s3://mybucket/data/listing/',
        access_key_id=access_key_id,
        secret_access_key=secret_access_key,
        compression='LZOP',
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
    CREDENTIALS '%s'
    CSV TRUNCATECOLUMNS DELIMITER ',' IGNOREHEADER 0 NULL '\0' LZOP
    EMPTYASNULL BLANKSASNULL
    """ % creds
    copy = dialect.CopyCommand(
        table=tbl,
        data_location='s3://mybucket/data/listing/',
        access_key_id=access_key_id,
        secret_access_key=secret_access_key,
        compression='LZOP',
        dangerous_null_delimiter=u'\000',
    )
    assert clean(expected_result) == clean(compile_query(copy))
