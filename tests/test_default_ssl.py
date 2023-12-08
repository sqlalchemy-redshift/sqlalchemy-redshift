from pathlib import Path

import sqlalchemy as sa
from sqlalchemy_redshift.dialect import (
    Psycopg2RedshiftDialectMixin, RedshiftDialect_redshift_connector
)


cwd = Path(__file__).parent.resolve()
sqlalchemy_redshift_dir = cwd.joinpath("..") / "sqlalchemy_redshift"
sqlalchemy_redshift_dir = sqlalchemy_redshift_dir.resolve()
CERT_PATH = str(sqlalchemy_redshift_dir / "redshift-ca-bundle.crt")


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
