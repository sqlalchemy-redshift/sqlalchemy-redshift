from pkg_resources import get_distribution
try:
    import psycopg2
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
