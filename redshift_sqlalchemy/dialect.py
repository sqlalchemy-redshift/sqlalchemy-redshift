from sqlalchemy.dialects.postgresql.psycopg2 import PGDialect_psycopg2
from sqlalchemy.engine import reflection
from sqlalchemy import util, exc
from sqlalchemy.types import VARCHAR, NullType
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql.expression import Executable, ClauseElement


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

    def _get_column_info(self, name, format_type, default,
                         notnull, domains, enums, schema):
        column_info = super(RedshiftDialect, self)._get_column_info(name, format_type, default, notnull, domains, enums, schema)
        if isinstance(column_info['type'], VARCHAR) and column_info['type'].length is None:
            column_info['type'] = NullType()
        return column_info


class UnloadFromSelect(Executable, ClauseElement):
    ''' Prepares a RedShift unload statement to drop a query to Amazon S3
    http://docs.aws.amazon.com/redshift/latest/dg/r_UNLOAD_command_examples.html
    '''
    def __init__(self, select, bucket, access_key, secret_key):
        ''' Initializes an UnloadFromSelect instance

        Args:
            self: An instance of UnloadFromSelect
            select: The select statement to be unloaded
            bucket: The Amazon S3 bucket where the result will be stored
            access_key: The Amazon Access Key ID
            secret_key: The Amazon Secret Access Key
        '''
        self.select = select
        self.bucket = bucket
        self.access_key = access_key
        self.secret_key = secret_key


@compiles(UnloadFromSelect)
def visit_unload_from_select(element, compiler, **kw):
    ''' Returns the actual sql query for the UnloadFromSelect class
    '''
    query = compiler.process(element.select)
    import pdb; pdb.set_trace()
    query_str = str(query).replace("'", "\'")
    return "unload ('%(query)s') to '%(bucket)s' credentials 'aws_access_key_id=%(access_key)s;aws_secret_access_key=%(secret_key)s' delimiter ',' addquotes" % {
        'query': query_str,
        'bucket': element.bucket,
        'access_key': element.access_key,
        'secret_key': element.secret_key,
    }
