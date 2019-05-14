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


def test_basic_unload_case():
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

    assert clean(compile_query(unload)) == clean(expected_result)


def test_iam_role():
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

    assert clean(compile_query(unload)) == clean(expected_result)


def test_all_redshift_options():
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

    assert clean(compile_query(unload)) == clean(expected_result)


def test_all_redshift_options_with_header():
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

    assert clean(compile_query(unload)) == clean(expected_result)


def test_csv_format():
    """Tests that UnloadFromSelect uses the format_as_csv option correctly."""

    unload = dialect.UnloadFromSelect(
        select=sa.select([sa.func.count(table.c.id)]),
        unload_location='s3://bucket/key',
        access_key_id=access_key_id,
        secret_access_key=secret_access_key,
        format_as_csv=True
    )

    expected_result = """
            UNLOAD ('SELECT count(t1.id) AS count_1 FROM t1')
            TO 's3://bucket/key'
            CREDENTIALS '{creds}'
            FORMAT AS CSV
        """.format(creds=creds)

    assert clean(compile_query(unload)) == clean(expected_result)
