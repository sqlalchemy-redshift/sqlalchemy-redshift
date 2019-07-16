from pkg_resources import get_distribution, parse_version

try:
    import psycopg2  # noqa: F401
    if get_distribution('psycopg2').parsed_version < parse_version(2.5):
        raise ImportError('Minimum required version for psycopg2 is 2.5')
        # requires sqlalchemy.sql.base.DialectKWArgs.dialect_options, new in
        # version 0.9.2
except ImportError:
    raise ImportError(
        'No module named psycopg2. Please install either '
        'psycopg2 or psycopg2-binary package for CPython '
        'or psycopg2cffi for Pypy.'
    )

__version__ = get_distribution('sqlalchemy-redshift').version

from sqlalchemy.dialects import registry

registry.register("redshift", "sqlalchemy_redshift.dialect", "RedshiftDialect")
registry.register(
    "redshift.psycopg2", "sqlalchemy_redshift.dialect", "RedshiftDialect"
)
