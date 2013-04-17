from sqlalchemy.dialects.postgresql.psycopg2 import PGDialect_psycopg2
from sqlalchemy.engine import reflection
from sqlalchemy import util, exc

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

    #def set_isolation_level(self, connection, level):
    #    from psycopg2 import extensions
    #    connection.set_isolation_level(extensions.ISOLATION_LEVEL_AUTOCOMMIT)

    @util.memoized_property
    def _isolation_lookup(self):
        extensions = __import__('psycopg2.extensions').extensions
        return {
            'READ COMMITTED': extensions.ISOLATION_LEVEL_READ_COMMITTED,
            'READ UNCOMMITTED': extensions.ISOLATION_LEVEL_READ_UNCOMMITTED,
            'REPEATABLE READ': extensions.ISOLATION_LEVEL_REPEATABLE_READ,
            'SERIALIZABLE': extensions.ISOLATION_LEVEL_SERIALIZABLE,
            'AUTOCOMMIT': extensions.ISOLATION_LEVEL_AUTOCOMMIT
        }

    def set_isolation_level(self, connection, level):
        try:
            level = self._isolation_lookup[level.replace('_', ' ')]
        except KeyError:
            raise exc.ArgumentError(
                "Invalid value '%s' for isolation_level. "
                "Valid isolation levels for %s are %s" %
                (level, self.name, ", ".join(self._isolation_lookup))
            )

        connection.set_isolation_level(level)
