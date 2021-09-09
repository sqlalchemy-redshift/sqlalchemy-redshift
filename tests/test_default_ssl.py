import sqlalchemy as sa
from pkg_resources import resource_filename


CERT_PATH = resource_filename("sqlalchemy_redshift", "redshift-ca-bundle.crt")


def test_ssl_args(redshift_dialect_flavor):
    engine = sa.create_engine('{}://test'.format(redshift_dialect_flavor))
    dialect = engine.dialect
    url = engine.url

    cargs, cparams = dialect.create_connect_args(url)

    assert cargs == []
    assert cparams.pop('host') == 'test'
    assert cparams.pop('sslmode') == 'verify-full'
    assert cparams.pop('sslrootcert') == CERT_PATH
    assert cparams == {}
