__version__ = '0.3.3'

from sqlalchemy.dialects import registry

registry.register("redshift", "redshift_sqlalchemy.dialect", "RedshiftDialect")
registry.register("redshift+psycopg2", "redshift_sqlalchemy.dialect", "RedshiftDialect")
