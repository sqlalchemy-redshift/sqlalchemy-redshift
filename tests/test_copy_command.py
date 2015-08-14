from redshift_sqlalchemy.dialect import CopyCommand
import re


def clean(query):
    return re.sub(r'\s+', ' ', query).strip()


def test_basic_copy_case():
    '''
        Tests that the simplest type of CopyCommand works
    '''
    expected_result = """
    COPY schema1.t1 FROM 's3://mybucket/data/listing/'
    CREDENTIALS 'aws_access_key_id=cookies;aws_secret_access_key=cookies'
    CSV TRUNCATECOLUMNS DELIMITER ',' IGNOREHEADER 0 EMPTYASNULL BLANKSASNULL;
    """

    copy = CopyCommand('schema1', 't1', 's3://mybucket/data/listing/', 'cookies', 'cookies')

    assert clean(expected_result) == clean(str(copy))
