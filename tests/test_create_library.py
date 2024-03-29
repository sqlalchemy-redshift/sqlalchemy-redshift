from sqlalchemy_redshift import dialect
from rs_sqla_test_utils.utils import clean, compile_query

# These are fake credentials for the tests.
ACCESS_KEY_ID = 'IO1IWSZL5YRFM3BEW256'
SECRET_ACCESS_KEY = 'A1Crw8=nJwEq+9SCgnwpYbqVSCnfB0cakn=lx4M1'
CREDS = (
    (
        'aws_access_key_id={access_key_id}'
        ';aws_secret_access_key={secret_access_key}'
    ).format(
        access_key_id=ACCESS_KEY_ID,
        secret_access_key=SECRET_ACCESS_KEY
    )
)


def test_basic(stub_redshift_dialect):
    name = 'asdfghjkl'
    where = 'https://www.example.com/libraries/extension.zip'

    expected_result = """
        CREATE LIBRARY "{name}"
        LANGUAGE pythonplu
        FROM '{where}'
        WITH CREDENTIALS AS '{creds}'
    """.format(name=name, where=where, creds=CREDS)

    cmd = dialect.CreateLibraryCommand(
        name, where, access_key_id=ACCESS_KEY_ID,
        secret_access_key=SECRET_ACCESS_KEY)
    assert clean(expected_result) == \
        clean(compile_query(cmd, stub_redshift_dialect))


def test_or_replace(stub_redshift_dialect):
    name = 'SomeLibrary'
    where = 's3://bucket/path/to/archive.zip'

    expected_result = """
        CREATE OR REPLACE LIBRARY "{name}"
        LANGUAGE pythonplu
        FROM '{where}'
        WITH CREDENTIALS AS '{creds}'
    """.format(name=name, where=where, creds=CREDS)

    cmd = dialect.CreateLibraryCommand(
        name, where, access_key_id=ACCESS_KEY_ID,
        secret_access_key=SECRET_ACCESS_KEY, replace=True)
    assert clean(expected_result) == \
        clean(compile_query(cmd, stub_redshift_dialect))


def test_region(stub_redshift_dialect):
    name = 'SomeLibrary'
    where = 's3://bucket/path/to/archive.zip'
    region = 'sa-east-1'

    expected_result = """
        CREATE LIBRARY "{name}"
        LANGUAGE pythonplu
        FROM '{where}'
        WITH CREDENTIALS AS '{creds}'
        REGION '{region}'
    """.format(name=name, where=where, creds=CREDS, region=region)

    cmd = dialect.CreateLibraryCommand(
        name, where, access_key_id=ACCESS_KEY_ID,
        secret_access_key=SECRET_ACCESS_KEY, region=region)
    assert clean(expected_result) == \
        clean(compile_query(cmd, stub_redshift_dialect))


def test_everything(stub_redshift_dialect):
    name = 'SomeLibrary'
    where = 's3://bucket/path/to/archive.zip'
    region = 'sa-east-1'

    expected_result = """
        CREATE OR REPLACE LIBRARY "{name}"
        LANGUAGE pythonplu
        FROM '{where}'
        WITH CREDENTIALS AS '{creds}'
        REGION '{region}'
    """.format(name=name, where=where, creds=CREDS, region=region)

    cmd = dialect.CreateLibraryCommand(
        name, where, access_key_id=ACCESS_KEY_ID,
        secret_access_key=SECRET_ACCESS_KEY, replace=True, region=region)
    assert clean(expected_result) == \
        clean(compile_query(cmd, stub_redshift_dialect))
