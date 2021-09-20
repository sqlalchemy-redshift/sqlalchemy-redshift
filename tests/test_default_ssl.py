import sqlalchemy as sa
from pkg_resources import resource_filename
from sqlalchemy_redshift.dialect import (
    Psycopg2RedshiftDialectMixin, RedshiftDialect_redshift_connector
)


CERT_PATH = resource_filename("sqlalchemy_redshift", "redshift-ca-bundle.crt")


def test_ssl_args(redshift_dialect_flavor):
    engine = sa.create_engine('{}://test'.format(redshift_dialect_flavor))
    dialect = engine.dialect
    url = engine.url

    cargs, cparams = dialect.create_connect_args(url)

    assert cargs == []
    assert cparams.pop('host') == 'test'
    assert cparams.pop('sslmode') == 'verify-full'
    if isinstance(dialect, Psycopg2RedshiftDialectMixin):
        assert cparams.pop('sslrootcert') == CERT_PATH
    elif isinstance(dialect, RedshiftDialect_redshift_connector):
        assert cparams.pop('ssl') is True
        assert cparams.pop('application_name') == 'sqlalchemy-redshift'
    assert cparams == {}
