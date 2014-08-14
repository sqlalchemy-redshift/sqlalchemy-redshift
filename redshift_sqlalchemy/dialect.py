from sqlalchemy import schema, util, exc
from sqlalchemy.dialects.postgresql.base import PGDDLCompiler
from sqlalchemy.dialects.postgresql.psycopg2 import PGDialect_psycopg2
from sqlalchemy.engine import reflection
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql.expression import BindParameter, Executable, ClauseElement
from sqlalchemy.types import VARCHAR, NullType, BigInteger, Integer


class RedShiftDDLCompiler(PGDDLCompiler):
    ''' Handles Redshift specific create table syntax.

    Users can specify the DISTSTYLE, DISTKEY, SORTKEY and ENCODE properties per
    table and per column.

    Table level properties can be set using the dialect specific syntax. For
    example, to specify a distkey and style you apply the following ::

        table = Table(metadata,
                      Column('id', Integer, primary_key=True),
                      Column('name', String),
                      redshift_diststyle="KEY",
                      redshift_distkey="id"
                      redshift_sortkey=["id", "name"]
                      )

    A single sortkey can be applied without a wrapping list ::

        table = Table(metadata,
                      Column('id', Integer, primary_key=True),
                      Column('name', String),
                      redshift_sortkey="id"
                      )

    Column level special syntax can also be applied using the column info
    dictionary. For example, we can specify the encode for a column ::

        table = Table(metadata,
                      Column('id', Integer, primary_key=True),
                      Column('name', String, info={"encode":"lzo"})
                      )

    We can also specify the distkey and sortkey options ::

        table = Table(metadata,
                      Column('id', Integer, primary_key=True),
                      Column('name', String,
                             info={"distkey":True, "sortkey":True})
                      )

    '''

    def post_create_table(self, table):
        text = ""
        info = table.dialect_options['redshift']
        diststyle = info.get('diststyle', None)
        if diststyle:
            diststyle = diststyle.upper()
            if diststyle not in ('EVEN', 'KEY', 'ALL'):
                raise exc.CompileError(
                               u"diststyle {0} is invalid".format(diststyle))
            text += " DISTSTYLE " + diststyle

        distkey = info.get('distkey', None)
        if distkey:
            text += " DISTKEY ({0})".format(distkey)

        sortkey = info.get('sortkey', None)
        if sortkey:
            if isinstance(sortkey, str):
                keys = (sortkey,)
            else:
                keys = sortkey
            text += " SORTKEY ({0})".format(", ".join(keys))
        return text

    def get_column_specification(self, column, **kwargs):
        colspec = self.preparer.format_column(column)

        colspec += " " + self.dialect.type_compiler.process(column.type)

        default = self.get_column_default_string(column)
        if default is not None:
            colspec += " DEFAULT " + default

        colspec += self._fetch_redshift_column_attributes(column)

        if not column.nullable:
            colspec += " NOT NULL"
        return colspec

    def _fetch_redshift_column_attributes(self, column):
        text = ""
        if not hasattr(column, 'info'):
            return text
        info = column.info
        identity = info.get('identity', None)
        if identity:
            text += " IDENTITY({0},{1})".format(identity[0], identity[1])

        encode = info.get('encode', None)
        if encode:
            text += " ENCODE " + encode

        distkey = info.get('distkey', None)
        if distkey:
            text += " DISTKEY"

        sortkey = info.get('sortkey', None)
        if sortkey:
            text += " SORTKEY"
        return text

class RedshiftDialect(PGDialect_psycopg2):
    name = 'redshift'
    ddl_compiler = RedShiftDDLCompiler

    construct_arguments = [
                            (schema.Index, {
                                "using": False,
                                "where": None,
                                "ops": {}
                            }),
                            (schema.Table, {
                                "ignore_search_path": False,
                                'diststyle': None,
                                'distkey': None,
                                'sortkey': None
                            }),
                           ]

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
    def __init__(self, select, bucket, access_key, secret_key, parallel='on'):
        ''' Initializes an UnloadFromSelect instance

        Args:
            self: An instance of UnloadFromSelect
            select: The select statement to be unloaded
            bucket: The Amazon S3 bucket where the result will be stored
            access_key: The Amazon Access Key ID
            secret_key: The Amazon Secret Access Key
            parallel: If 'ON' the result will be written to multiple files. If
                'OFF' the result will write to one (1) file up to 6.2GB before
                splitting
        '''
        self.select = select
        self.bucket = bucket
        self.access_key = access_key
        self.secret_key = secret_key
        self.parallel = parallel


@compiles(UnloadFromSelect)
def visit_unload_from_select(element, compiler, **kw):
    ''' Returns the actual sql query for the UnloadFromSelect class
    '''
    return "unload ('%(query)s') to '%(bucket)s' credentials 'aws_access_key_id=%(access_key)s;aws_secret_access_key=%(secret_key)s' delimiter ',' addquotes allowoverwrite parallel %(parallel)s" % {
        'query': compiler.process(element.select, unload_select=True, literal_binds=True),
        'bucket': element.bucket,
        'access_key': element.access_key,
        'secret_key': element.secret_key,
        'parallel': element.parallel,
    }

@compiles(BindParameter)
def visit_bindparam(bindparam, compiler, **kw):
    #print bindparam
    res = compiler.visit_bindparam(bindparam, **kw)
    if 'unload_select' in kw:
        #process param and return
        res = res.replace("'", "\\'")
        res = res.replace('%', '%%')
        return res
    else:
        return res
