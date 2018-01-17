__author__ = 'haleemur'

import re
from sqlalchemy_redshift import dialect


def clean(query):
    query = re.sub(r'\s+ENCODE\s+\w+', '', query)
    query = re.sub(r'\s+CONSTRAINT\s+[a-zA-Z0-9_".]+', '', query)
    return re.sub(r'\s+', ' ', query).strip()


def compile_query(q):
    return str(q.compile(
        dialect=dialect.RedshiftDialect(),
        compile_kwargs={'literal_binds': True})
    )
