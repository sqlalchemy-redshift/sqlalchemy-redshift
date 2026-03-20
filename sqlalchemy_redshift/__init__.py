from importlib.metadata import version, PackageNotFoundError

try:
    __version__ = version('sqlalchemy-redshift')
except PackageNotFoundError:
    __version__ = '0.0.0'

from sqlalchemy.dialects import registry

registry.register(
    "redshift", "sqlalchemy_redshift.dialect",
    "RedshiftDialect_psycopg2"
)
registry.register(
    "redshift.psycopg2", "sqlalchemy_redshift.dialect",
    "RedshiftDialect_psycopg2"
)
registry.register(
    'redshift+psycopg2cffi', 'sqlalchemy_redshift.dialect',
    'RedshiftDialect_psycopg2cffi',
)
registry.register(
    "redshift+redshift_connector", "sqlalchemy_redshift.dialect",
    "RedshiftDialect_redshift_connector"
)
