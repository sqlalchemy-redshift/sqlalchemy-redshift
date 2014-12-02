from unittest import TestCase
from redshift_sqlalchemy.dialect import CopyCommand
import re


class TestCopyCommand(TestCase):

    def setUp(self):
        pass

    def test_basic_copy_case(self):
        '''
            Tests that the simplest type of CopyCommand works
        '''
        expected_result = re.sub(r'\s+', ' ',
                                  "COPY schema1.t1 FROM 's3://mybucket/data/listing/' "
                                  "CREDENTIALS 'aws_access_key_id=cookies;aws_secret_access_key=cookies' "
                                  "CSV TRUNCATECOLUMNS DELIMITER ',' IGNOREHEADER 0 EMPTYASNULL BLANKSASNULL;").strip()
        copy = CopyCommand('schema1', 't1', 's3://mybucket/data/listing/', 'cookies', 'cookies')

        copy_str = re.sub(r'\s+', ' ', str(copy)).strip()

        self.assertEqual(expected_result, copy_str)
