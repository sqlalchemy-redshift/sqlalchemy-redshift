import pytest

import sqlalchemy as sa

from sqlalchemy.engine.url import URL


@pytest.fixture(params=[
    'redshift', 'redshift+pg8000', 'redshift+psycopg2',
    'redshift+psycopg2cffi',
])
def redshift_engine(request):
    return sa.create_engine(URL(drivername=request.param))

@pytest.fixture
def redshift_dialect(redshift_engine):
    return redshift_engine.dialect
