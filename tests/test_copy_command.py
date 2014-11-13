from unittest import TestCase
from redshift_sqlalchemy.dialect import CopyCommand


class TestCopyCommand(TestCase):
    def setUp(self):
        pass

    def test_basic_copy_case(self):
        ''' Tests that the simplest type of CopyCommand works
        '''
        expected_result = "COPY schema1.t1 FROM 's3://mybucket/data/listing/'\n" \
                          "           CREDENTIALS 'aws_access_key_id=cookies;aws_secret_access_key=cookies'\n" \
                          "           CSV\n           TRUNCATECOLUMNS\n           DELIMITER ','\n" \
                          "           IGNOREHEADER 0\n           NULL '---'\n           EMPTYASNULL\n" \
                          "           BLANKSASNULL;"
        insert = CopyCommand('schema1', 't1', 's3://mybucket/data/listing/', 'cookies', 'cookies')
        self.assertEqual(expected_result, str(insert).strip())
