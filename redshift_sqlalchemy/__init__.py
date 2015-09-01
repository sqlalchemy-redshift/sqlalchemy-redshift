from pkg_resources import get_distribution

__version__ = get_distribution('sqlalchemy-redshift').version

from sqlalchemy.dialects import registry

registry.register("redshift", "redshift_sqlalchemy.dialect", "RedshiftDialect")
registry.register(
    "redshift+psycopg2", "redshift_sqlalchemy.dialect", "RedshiftDialect"
)
