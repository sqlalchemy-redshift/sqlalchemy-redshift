from sqlalchemy.dialects.postgresql.psycopg2 import PGDialect_psycopg2
from sqlalchemy.engine import reflection

class RedshiftDialect(PGDialect_psycopg2):
    @reflection.cache
    def get_pk_constraint(self, connection, table_name, schema=None, **kw):
        """
        Constraints in redshift are informational only. This allows reflection to work
        """
        return {'constrained_columns': [], 'name': ''}

    @reflection.cache
    def get_indexes(self, connection, table_name, schema, **kw):
        """
        Redshift does not use traditional indexes.
        """
        return []

    def set_isolation_level(self, connection, level):
        from psycopg2 import extensions
        connection.set_isolation_level(extensions.ISOLATION_LEVEL_AUTOCOMMIT)

