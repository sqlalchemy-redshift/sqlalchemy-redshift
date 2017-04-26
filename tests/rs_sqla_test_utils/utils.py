__author__ = 'haleemur'

import re
from sqlalchemy_redshift import dialect


def clean(query):
    encodings_removed = re.sub(r'\s+ENCODE\s+\w+', '', query)
    return re.sub(r'\s+', ' ', encodings_removed).strip()


def compile_query(q):
    return str(q.compile(
        dialect=dialect.RedshiftDialect(),
        compile_kwargs={'literal_binds': True})
    )
