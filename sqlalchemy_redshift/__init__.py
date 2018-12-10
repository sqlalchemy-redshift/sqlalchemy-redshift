from pkg_resources import get_distribution

__version__ = get_distribution('sqlalchemy-redshift').version

from sqlalchemy.dialects import registry

registry.register("redshift", "sqlalchemy_redshift.dialect", "RedshiftDialect")
registry.register(
    "redshift.psycopg2", "sqlalchemy_redshift.dialect", "RedshiftDialect"
)
