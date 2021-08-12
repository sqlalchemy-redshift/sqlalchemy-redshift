import pytest
import sqlalchemy as sa

from sqlalchemy_redshift import dialect

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


table = sa.Table(
    't1', sa.MetaData(),
    sa.Column('id', sa.Integer, primary_key=True),
    sa.Column('name', sa.Unicode),
)


def test_basic_unload_case(stub_redshift_dialect):
    """Tests that the simplest type of UnloadFromSelect works."""

    unload = dialect.UnloadFromSelect(
        select=sa.select([sa.func.count(table.c.id)]),
        unload_location='s3://bucket/key',
        access_key_id=access_key_id,
        secret_access_key=secret_access_key,
    )

    expected_result = """
        UNLOAD ('SELECT count(t1.id) AS count_1 FROM t1')
        TO 's3://bucket/key'
        CREDENTIALS '{creds}'
    """.format(creds=creds)

    assert clean(compile_query(unload, stub_redshift_dialect)) == clean(expected_result)


def test_iam_role(stub_redshift_dialect):
    """Tests the use of iam role instead of access keys."""

    aws_account_id = '000123456789'
    iam_role_name = 'redshiftrole'
    creds = 'aws_iam_role=arn:aws:iam::{0}:role/{1}'.format(
        aws_account_id,
        iam_role_name,
    )

    unload = dialect.UnloadFromSelect(
        select=sa.select([sa.func.count(table.c.id)]),
        unload_location='s3://bucket/key',
        aws_account_id=aws_account_id,
        iam_role_name=iam_role_name,
    )

    expected_result = """
        UNLOAD ('SELECT count(t1.id) AS count_1 FROM t1')
        TO 's3://bucket/key'
        CREDENTIALS '{creds}'
    """.format(creds=creds)

    assert clean(compile_query(unload, stub_redshift_dialect)) == clean(expected_result)


def test_iam_role_partition(stub_redshift_dialect):
    """Tests the use of iam role with a custom partition"""

    aws_partition = 'aws-us-gov'
    aws_account_id = '000123456789'
    iam_role_name = 'redshiftrole'
    creds = 'aws_iam_role=arn:{0}:iam::{1}:role/{2}'.format(
        aws_partition,
        aws_account_id,
        iam_role_name,
    )

    unload = dialect.UnloadFromSelect(
        select=sa.select([sa.func.count(table.c.id)]),
        unload_location='s3://bucket/key',
        aws_partition=aws_partition,
        aws_account_id=aws_account_id,
        iam_role_name=iam_role_name,
    )

    expected_result = """
        UNLOAD ('SELECT count(t1.id) AS count_1 FROM t1')
        TO 's3://bucket/key'
        CREDENTIALS '{creds}'
    """.format(creds=creds)

    assert clean(compile_query(unload, stub_redshift_dialect)) == clean(expected_result)


def test_iam_role_partition_validation():
    """Tests the use of iam role with an invalid partition"""

    aws_partition = 'aws-invalid'
    aws_account_id = '000123456789'
    iam_role_name = 'redshiftrole'

    with pytest.raises(ValueError):
        dialect.UnloadFromSelect(
            select=sa.select([sa.func.count(table.c.id)]),
            unload_location='s3://bucket/key',
            aws_partition=aws_partition,
            aws_account_id=aws_account_id,
            iam_role_name=iam_role_name,
        )


def test_iam_role_arns_list(stub_redshift_dialect):
    """Tests the use of multiple iam role arns instead of access keys."""

    iam_role_arns = [
        'arn:aws:iam::000123456789:role/redshiftrole',
        'arn:aws:iam::000123456789:role/redshiftrole2',
    ]
    creds = 'aws_iam_role=arn:aws:iam::000123456789:role/redshiftrole,' \
            'arn:aws:iam::000123456789:role/redshiftrole2'

    unload = dialect.UnloadFromSelect(
        select=sa.select([sa.func.count(table.c.id)]),
        unload_location='s3://bucket/key',
        iam_role_arns=iam_role_arns,
    )

    expected_result = """
        UNLOAD ('SELECT count(t1.id) AS count_1 FROM t1')
        TO 's3://bucket/key'
        CREDENTIALS '{creds}'
    """.format(creds=creds)

    assert clean(compile_query(unload, stub_redshift_dialect)) == clean(expected_result)


def test_iam_role_arns_single(stub_redshift_dialect):
    """Tests the use of a single iam role arn instead of access keys."""

    iam_role_arns = 'arn:aws:iam::000123456789:role/redshiftrole'
    creds = 'aws_iam_role=arn:aws:iam::000123456789:role/redshiftrole'

    unload = dialect.UnloadFromSelect(
        select=sa.select([sa.func.count(table.c.id)]),
        unload_location='s3://bucket/key',
        iam_role_arns=iam_role_arns,
    )

    expected_result = """
        UNLOAD ('SELECT count(t1.id) AS count_1 FROM t1')
        TO 's3://bucket/key'
        CREDENTIALS '{creds}'
    """.format(creds=creds)

    assert clean(compile_query(unload, stub_redshift_dialect)) == clean(expected_result)


def test_all_redshift_options(stub_redshift_dialect):
    """Tests that UnloadFromSelect handles all options correctly."""

    unload = dialect.UnloadFromSelect(
        sa.select([sa.func.count(table.c.id)]),
        unload_location='s3://bucket/key',
        access_key_id=access_key_id,
        secret_access_key=secret_access_key,
        manifest=True,
        delimiter=',',
        fixed_width=[('count_1', 50), ],
        encrypted=True,
        gzip=True,
        add_quotes=True,
        null='---',
        escape=True,
        allow_overwrite=True,
        parallel=False,
        region='us-west-2',
        max_file_size=10 * 1024**2,
    )

    expected_result = """
        UNLOAD ('SELECT count(t1.id) AS count_1 FROM t1')
        TO 's3://bucket/key'
        CREDENTIALS '{creds}'
        MANIFEST
        DELIMITER AS ','
        ENCRYPTED
        FIXEDWIDTH AS 'count_1:50'
        GZIP
        ADDQUOTES
        NULL AS '---'
        ESCAPE
        ALLOWOVERWRITE
        PARALLEL OFF
        REGION 'us-west-2'
        MAXFILESIZE 10.0 MB
    """.format(creds=creds)

    assert clean(compile_query(unload, stub_redshift_dialect)) == clean(expected_result)


def test_all_redshift_options_with_header(stub_redshift_dialect):
    """Tests that UnloadFromSelect handles all options correctly."""

    unload = dialect.UnloadFromSelect(
        sa.select([sa.func.count(table.c.id)]),
        unload_location='s3://bucket/key',
        access_key_id=access_key_id,
        secret_access_key=secret_access_key,
        manifest=True,
        header=True,
        delimiter=',',
        encrypted=True,
        gzip=True,
        add_quotes=True,
        null='---',
        escape=True,
        allow_overwrite=True,
        parallel=False,
        region='ap-northeast-2',
        max_file_size=10 * 1024 ** 2,
    )

    expected_result = """
        UNLOAD ('SELECT count(t1.id) AS count_1 FROM t1')
        TO 's3://bucket/key'
        CREDENTIALS '{creds}'
        MANIFEST
        HEADER
        DELIMITER AS ','
        ENCRYPTED
        GZIP
        ADDQUOTES
        NULL AS '---'
        ESCAPE
        ALLOWOVERWRITE
        PARALLEL OFF
        REGION 'ap-northeast-2'
        MAXFILESIZE 10.0 MB
    """.format(creds=creds)

    assert clean(compile_query(unload, stub_redshift_dialect)) == clean(expected_result)


def test_csv_format__basic(stub_redshift_dialect):
    """Tests that UnloadFromSelect uses the format option correctly."""

    unload = dialect.UnloadFromSelect(
        select=sa.select([sa.func.count(table.c.id)]),
        unload_location='s3://bucket/key',
        access_key_id=access_key_id,
        secret_access_key=secret_access_key,
        format=dialect.Format.csv
    )

    expected_result = """
            UNLOAD ('SELECT count(t1.id) AS count_1 FROM t1')
            TO 's3://bucket/key'
            CREDENTIALS '{creds}'
            FORMAT AS CSV
        """.format(creds=creds)

    assert clean(compile_query(unload, stub_redshift_dialect)) == clean(expected_result)


@pytest.mark.parametrize('delimiter,fixed_width', (
    ('\t', None),
    (None, 'id:8,name:32'),
    (';', 'id:8,name:32'),
))
def test_csv_format__bad_options_crash(delimiter, fixed_width, stub_redshift_dialect):
    """Test that UnloadFromSelect crashes if you try to use DELIMITER and/or
    FIXEDWIDTH with the CSV format.
    """
    unload = dialect.UnloadFromSelect(
        select=sa.select([sa.func.count(table.c.id)]),
        unload_location='s3://bucket/key',
        access_key_id=access_key_id,
        secret_access_key=secret_access_key,
        format=dialect.Format.csv,
        delimiter=delimiter,
        fixed_width=fixed_width
    )

    with pytest.raises(ValueError):
        compile_query(unload, stub_redshift_dialect)


def test_parquet_format__basic(stub_redshift_dialect):
    """Basic successful test of unloading with the Parquet format."""
    unload = dialect.UnloadFromSelect(
        select=sa.select([sa.func.count(table.c.id)]),
        unload_location='s3://bucket/key',
        access_key_id=access_key_id,
        secret_access_key=secret_access_key,
        format=dialect.Format.parquet,
    )

    expected_result = """
        UNLOAD ('SELECT count(t1.id) AS count_1 FROM t1')
        TO 's3://bucket/key'
        CREDENTIALS '{creds}'
        FORMAT AS PARQUET
    """.format(creds=creds)

    assert clean(compile_query(unload, stub_redshift_dialect)) == clean(expected_result)


@pytest.mark.parametrize('kwargs', (
    {'delimiter': '\t'},
    {'fixed_width': 'id:8,name:32'},
    {'gzip': True},
    {'add_quotes': True, 'escape': True},
    {'null': '\\N'},
    {'header': True},
))
def test_parquet_format__bad_options_crash(kwargs, stub_redshift_dialect):
    """Verify we crash if we use the Parquet format with a bad option."""
    unload = dialect.UnloadFromSelect(
        select=sa.select([sa.func.count(table.c.id)]),
        unload_location='s3://bucket/key',
        access_key_id=access_key_id,
        secret_access_key=secret_access_key,
        format=dialect.Format.parquet,
        **kwargs
    )

    with pytest.raises(ValueError):
        compile_query(unload, stub_redshift_dialect)
