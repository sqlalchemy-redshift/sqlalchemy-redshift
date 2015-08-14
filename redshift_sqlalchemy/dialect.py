from sqlalchemy import schema, util, exc
from sqlalchemy.dialects.postgresql.base import PGDDLCompiler
from sqlalchemy.dialects.postgresql import (
    pg8000, psycopg2, psycopg2cffi, pypostgresql, zxjdbc,
)
from sqlalchemy.engine import reflection
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql.expression import BindParameter, Executable, ClauseElement
from sqlalchemy.types import VARCHAR, NullType


try:
    from alembic.ddl import postgresql
except ImportError:
    pass
else:
    from alembic.ddl.base import RenameTable
    compiles(RenameTable, 'redshift')(postgresql.visit_rename_table)

    class RedshiftImpl(postgresql.PostgresqlImpl):
        __dialect__ = 'redshift'


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
                    u"diststyle {0} is invalid".format(diststyle)
                )
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


class RedshiftDialectMixin(object):
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
        Constraints in redshift are informational only. This allows reflection
        to work.
        """
        return {'constrained_columns': [], 'name': ''}

    @reflection.cache
    def get_indexes(self, connection, table_name, schema, **kw):
        """
        Redshift does not use traditional indexes.
        """
        return []

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

    def _get_column_info(self, *args, **kwargs):
        column_info = super(RedshiftDialectMixin, self)._get_column_info(
            *args,
            **kwargs
        )
        if isinstance(column_info['type'], VARCHAR):
            if column_info['type'].length is None:
                column_info['type'] = NullType()

        return column_info


class PsycopgRedshiftDialect(RedshiftDialectMixin, psycopg2.dialect):
    pass


class PsycopgCFFIRedshiftDialect(RedshiftDialectMixin, psycopg2cffi.dialect):
    pass


class Pg8000RedshiftDialect(RedshiftDialectMixin, pg8000.dialect):
    pass


class PypostgresqlRedshiftDialect(RedshiftDialectMixin, pypostgresql.dialect):
    pass


class ZxjdbcRedshiftDialect(RedshiftDialectMixin, zxjdbc.dialect):
    pass


class UnloadFromSelect(Executable, ClauseElement):
    ''' Prepares a RedShift unload statement to drop a query to Amazon S3
    http://docs.aws.amazon.com/redshift/latest/dg/r_UNLOAD_command_examples.html
    '''
    def __init__(self, select, unload_location, access_key, secret_key, session_token='', options={}):
        ''' Initializes an UnloadFromSelect instance

        Args:
            self: An instance of UnloadFromSelect
            select: The select statement to be unloaded
            unload_location: The Amazon S3 bucket where the result will be stored
            access_key - AWS Access Key (required)
            secret_key - AWS Secret Key (required)
            session_token - AWS STS Session Token (optional)
            options - Set of optional parameters to modify the UNLOAD sql
                parallel: If 'ON' the result will be written to multiple files. If
                    'OFF' the result will write to one (1) file up to 6.2GB before
                    splitting
                add_quotes: Boolean value for ADDQUOTES; defaults to True
                null_as: optional string that represents a null value in unload output
                delimiter - File delimiter. Defaults to ','
        '''
        self.select = select
        self.unload_location = unload_location
        self.access_key = access_key
        self.secret_key = secret_key
        self.session_token = session_token
        self.options = options


@compiles(UnloadFromSelect)
def visit_unload_from_select(element, compiler, **kw):
    ''' Returns the actual sql query for the UnloadFromSelect class

    '''
    return """
           UNLOAD ('%(query)s') TO '%(unload_location)s'
           CREDENTIALS 'aws_access_key_id=%(access_key)s;aws_secret_access_key=%(secret_key)s%(session_token)s'
           DELIMITER '%(delimiter)s'
           %(add_quotes)s
           %(null_as)s
           ALLOWOVERWRITE
           PARALLEL %(parallel)s;
           """ % \
           {'query': compiler.process(element.select, unload_select=True, literal_binds=True),
            'unload_location': element.unload_location,
            'access_key': element.access_key,
            'secret_key': element.secret_key,
            'session_token': ';token=%s' % element.session_token if element.session_token else '',
            'add_quotes': 'ADDQUOTES' if bool(element.options.get('add_quotes', True)) else '',
            'null_as': ("NULL '%s'" % element.options.get('null_as')) if element.options.get('null_as') else '',
            'delimiter': element.options.get('delimiter', ','),
            'parallel': element.options.get('parallel', 'ON')}


class CopyCommand(Executable, ClauseElement):
    ''' Prepares a RedShift COPY statement
    '''
    def __init__(self, schema_name, table_name, data_location, access_key, secret_key, session_token='', options={}):
        ''' Initializes a CopyCommand instance

        Args:
            self: An instance of CopyCommand
            schema_name - Schema associated with the table_name
            table_name: The table to copy the data into
            data_location The Amazon S3 location from where to copy - or a manifest file if 'manifest' option is used
            access_key - AWS Access Key (required)
            secret_key - AWS Secret Key (required)
            session_token - AWS STS Session Token (optional)
            options - Set of optional parameters to modify the COPY sql
                delimiter - File delimiter; defaults to ','
                ignore_header - Integer value of number of lines to skip at the start of each file
                null - Optional string value denoting what to interpret as a NULL value from the file
                manifest - Boolean value denoting whether data_location is a manifest file; defaults to False
                empty_as_null - Boolean value denoting whether to load VARCHAR fields with
                                empty values as NULL instead of empty string; defaults to True
                blanks_as_null - Boolean value denoting whether to load VARCHAR fields with
                                 whitespace only values as NULL instead of whitespace; defaults to True
        '''
        self.schema_name = schema_name
        self.table_name = table_name
        self.data_location = data_location
        self.access_key = access_key
        self.secret_key = secret_key
        self.session_token = session_token
        self.options = options


@compiles(CopyCommand)
def visit_copy_command(element, compiler, **kw):
    ''' Returns the actual sql query for the CopyCommand class
    '''
    return """
           COPY %(schema_name)s.%(table_name)s FROM '%(data_location)s'
           CREDENTIALS 'aws_access_key_id=%(access_key)s;aws_secret_access_key=%(secret_key)s%(session_token)s'
           CSV
           TRUNCATECOLUMNS
           DELIMITER '%(delimiter)s'
           IGNOREHEADER %(ignore_header)s
           %(null)s
           %(manifest)s
           %(empty_as_null)s
           %(blanks_as_null)s;
           """ % \
           {'schema_name': element.schema_name,
            'table_name': element.table_name,
            'data_location': element.data_location,
            'access_key': element.access_key,
            'secret_key': element.secret_key,
            'session_token': ';token=%s' % element.session_token if element.session_token else '',
            'null': ("NULL '%s'" % element.options.get('null')) if element.options.get('null') else '',
            'delimiter': element.options.get('delimiter', ','),
            'ignore_header': element.options.get('ignore_header', 0),
            'manifest': 'MANIFEST' if bool(element.options.get('manifest', False)) else '',
            'empty_as_null': 'EMPTYASNULL' if bool(element.options.get('empty_as_null', True)) else '',
            'blanks_as_null': 'BLANKSASNULL' if bool(element.options.get('blanks_as_null', True)) else ''}


@compiles(BindParameter)
def visit_bindparam(bindparam, compiler, **kw):
    res = compiler.visit_bindparam(bindparam, **kw)
    if 'unload_select' in kw:
        # process param and return
        res = res.replace("'", "\\'")
        res = res.replace('%', '%%')
        return res
    else:
        return res
