import sqlalchemy as sa
from pkg_resources import resource_filename
import pytest
from tests.conftest import redshift_dialect_flavors

CERT_PATH = resource_filename("sqlalchemy_redshift", "redshift-ca-bundle.crt")


@pytest.mark.parametrize("dialect_name", redshift_dialect_flavors)
def test_ssl_args(dialect_name):
    engine = sa.create_engine('{}://test'.format(dialect_name))
    dialect = engine.dialect
    url = engine.url

    cargs, cparams = dialect.create_connect_args(url)

    assert cargs == []
    assert cparams.pop('host') == 'test'
    assert cparams.pop('sslmode') == 'verify-full'
    assert cparams.pop('sslrootcert') == CERT_PATH
    assert cparams == {}
