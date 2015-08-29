import pkg_resources
import re
import numbers

import sqlalchemy as sa
from sqlalchemy import schema, util, exc
from sqlalchemy.dialects.postgresql.base import PGDDLCompiler, PGCompiler
from sqlalchemy.dialects.postgresql.psycopg2 import PGDialect_psycopg2
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


class RedshiftCompiler(PGCompiler):

    def visit_now_func(self, fn, **kw):
        return "SYSDATE"


class RedShiftDDLCompiler(PGDDLCompiler):
    """
    Handles Redshift specific create table syntax.

    Users can specify the DISTSTYLE, DISTKEY, SORTKEY and ENCODE properties per
    table and per column.

    Table level properties can be set using the dialect specific syntax. For
    example, to specify a distkey and style you apply the following ::

    >>> import sqlalchemy as sa
    >>> from sqlalchemy.schema import CreateTable
    >>> engine = sa.create_engine('redshift+psycopg2://example')
    >>> metadata = sa.MetaData()
    >>> user = sa.Table(
    ...     'user',
    ...     metadata,
    ...     sa.Column('id', sa.Integer, primary_key=True),
    ...     sa.Column('name', sa.String),
    ...     redshift_diststyle='KEY',
    ...     redshift_distkey='id',
    ...     redshift_sortkey=['id', 'name'],
    ... )
    >>> print(CreateTable(user).compile(engine))
    <BLANKLINE>
    CREATE TABLE "user" (
        id INTEGER NOT NULL,
        name VARCHAR,
        PRIMARY KEY (id)
    ) DISTSTYLE KEY DISTKEY (id) SORTKEY (id, name)
    <BLANKLINE>
    <BLANKLINE>

    A single sortkey can be applied without a wrapping list ::

    >>> customer = sa.Table(
    ...     'customer',
    ...     metadata,
    ...     sa.Column('id', sa.Integer, primary_key=True),
    ...     sa.Column('name', sa.String),
    ...     redshift_sortkey='id',
    ... )
    >>> print(CreateTable(customer).compile(engine))
    <BLANKLINE>
    CREATE TABLE customer (
        id INTEGER NOT NULL,
        name VARCHAR,
        PRIMARY KEY (id)
    ) SORTKEY (id)
    <BLANKLINE>
    <BLANKLINE>

    Column level special syntax can also be applied using the column info
    dictionary. For example, we can specify the encode for a column ::

    >>> product = sa.Table(
    ...     'product',
    ...     metadata,
    ...     sa.Column('id', sa.Integer, primary_key=True),
    ...     sa.Column('name', sa.String, info={'encode': 'lzo'})
    ... )
    >>> print(CreateTable(product).compile(engine))
    <BLANKLINE>
    CREATE TABLE product (
        id INTEGER NOT NULL,
        name VARCHAR ENCODE lzo,
        PRIMARY KEY (id)
    )
    <BLANKLINE>
    <BLANKLINE>

    We can also specify the distkey and sortkey options ::

    >>> sku = sa.Table(
    ...     'sku',
    ...     metadata,
    ...     sa.Column('id', sa.Integer, primary_key=True),
    ...     sa.Column(
    ...         'name', sa.String, info={'distkey': True, 'sortkey': True}
    ...     )
    ... )
    >>> print(CreateTable(sku).compile(engine))
    <BLANKLINE>
    CREATE TABLE sku (
        id INTEGER NOT NULL,
        name VARCHAR DISTKEY SORTKEY,
        PRIMARY KEY (id)
    )
    <BLANKLINE>
    <BLANKLINE>
    """

    def post_create_table(self, table):
        text = ""
        diststyle = table.kwargs.get('redshift_diststyle', None)
        if diststyle:
            diststyle = diststyle.upper()
            if diststyle not in ('EVEN', 'KEY', 'ALL'):
                raise exc.CompileError(
                    u"diststyle {0} is invalid".format(diststyle)
                )
            text += " DISTSTYLE " + diststyle

        distkey = table.kwargs.get('redshift_distkey', None)
        if distkey:
            text += " DISTKEY ({0})".format(distkey)

        sortkey = table.kwargs.get('redshift_sortkey', None)
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

    statement_compiler = RedshiftCompiler
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
        column_info = super(RedshiftDialect, self)._get_column_info(
            *args,
            **kwargs
        )
        if isinstance(column_info['type'], VARCHAR):
            if column_info['type'].length is None:
                column_info['type'] = NullType()

        return column_info

    def create_connect_args(self, *args, **kwargs):
        default_args = {
            'sslmode': 'verify-full',
            'sslrootcert': pkg_resources.resource_filename(
                __name__,
                'redshift-ssl-ca-cert.pem'
            ),
        }
        cargs, cparams = super(RedshiftDialect, self).create_connect_args(
            *args, **kwargs
        )
        default_args.update(cparams)
        return cargs, default_args


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


# At the time of this implementation, no specification for a session token was
# found. After looking at a few session tokens they appear to be the same as
# the aws_secret_access_key pattern, but much longer. An example token can be
# found here: http://docs.aws.amazon.com/STS/latest/APIReference/API_GetSessionToken.html
# The regexs for access keys can be found here: http://blogs.aws.amazon.com/security/blog/tag/key+rotation
creds_rx = re.compile(r"""
    ^aws_access_key_id=[A-Z0-9]{20};
    aws_secret_access_key=[A-Za-z0-9/+=]{40}
    (?:;token=[A-Za-z0-9/+=]+)?$
""", re.VERBOSE)


class CopyCommand(Executable, ClauseElement):
    """
    Prepares a Redshift COPY statement.

    Parameters
    ----------
    table : sqlalchemy.Table
        The table to copy data into
    data_location : str
        The Amazon S3 location from where to copy, or a manifest file if
        the `manifest` option is used
    access_key : str
    secret_key : str
    session_token : str, optional
    delimiter : File delimiter, optional
        defaults to ','
    ignore_header : int, optional
        Integer value of number of lines to skip at the start of each file
    dangerous_null_delimiter : str, optional
        Optional string value denoting what to interpret as a NULL value from
        the file. Note that this parameter *is not properly quoted* due to a
        difference between redshift's and postgres's COPY commands
        interpretation of strings. For example, null bytes must be passed to
        redshift's ``NULL`` verbatim as ``'\\0'`` whereas postgres's ``NULL``
        accepts ``'\\x00'``.
    manifest : bool, optional
        Boolean value denoting whether data_location is a manifest file.
    empty_as_null : bool, optional
        Boolean value denoting whether to load VARCHAR fields with empty
        values as NULL instead of empty string
    blanks_as_null : bool, optional
        Boolean value denoting whether to load VARCHAR fields with whitespace
        only values as NULL instead of whitespace
    format : str, optional
        CSV, JSON, or AVRO. Indicates the type of file to copy from.
    compression : str, optional
        GZIP, LZOP, indicates the type of compression of the file to copy
    """
    formats = ['CSV', 'JSON', 'AVRO']
    compression_types = ['GZIP', 'LZOP']

    def __init__(self, table, data_location, access_key_id, secret_access_key,
                 session_token=None, delimiter=',', ignore_header=0,
                 dangerous_null_delimiter=None, manifest=False,
                 empty_as_null=True,
                 blanks_as_null=True, format='CSV', compression=None):

        credentials = 'aws_access_key_id={0};aws_secret_access_key={1}'.format(
            access_key_id,
            secret_access_key
        )

        if session_token is not None:
            credentials += ';token={0}'.format(session_token)

        if not creds_rx.match(credentials):
            raise ValueError('credentials must match the following'
                             ' format:\n'
                             'aws_access_key_id=<access-key-id>;'
                             'aws_secret_access_key=<secret-access-key>'
                             '[;token=<temporary-session-token>]\ngot %r' %
                             credentials)

        if len(delimiter) != 1:
            raise ValueError('"delimiter" parameter must be a single '
                             'character')

        if not isinstance(ignore_header, numbers.Integral):
            raise TypeError('"ignore_header" parameter should be an integer')

        if format not in self.formats:
            raise ValueError('"format" parameter must be one of %s' %
                             self.formats)

        if compression is not None and compression not in self.compression_types:
            raise ValueError('"compression" parameter must be one of %s' %
                             self.compression_types)

        self.table = table
        self.data_location = data_location
        self.credentials = credentials
        self.delimiter = delimiter
        self.ignore_header = ignore_header
        self.dangerous_null_delimiter = dangerous_null_delimiter
        self.manifest = manifest
        self.empty_as_null = empty_as_null
        self.blanks_as_null = blanks_as_null
        self.format = format
        self.compression = compression or ''


def _tablename(t, compiler):
    name = compiler.preparer.quote(t.name)
    if t.schema is not None:
        return '%s.%s' % (compiler.preparer.quote_schema(t.schema), name)
    else:
        return name


@compiles(CopyCommand)
def visit_copy_command(element, compiler, **kw):
    ''' Returns the actual sql query for the CopyCommand class
    '''
    qs = """COPY {table} FROM :data_location
    CREDENTIALS :credentials
    {format}
    TRUNCATECOLUMNS
    DELIMITER :delimiter
    IGNOREHEADER :ignore_header
    {null}
    {manifest}
    {compression}
    {empty_as_null}
    {blanks_as_null}
    """.format(table=_tablename(element.table, compiler),
               format=element.format,
               manifest='MANIFEST' if element.manifest else '',
               compression=element.compression,
               empty_as_null='EMPTYASNULL' if element.empty_as_null else '',
               blanks_as_null='BLANKSASNULL' if element.blanks_as_null else '',
               ignore_header=element.ignore_header,
               null=(("NULL '%s'" % element.dangerous_null_delimiter)
                     if element.dangerous_null_delimiter is not None else ''))

    return compiler.process(
        sa.text(qs).bindparams(
            sa.bindparam('data_location',
                         value=element.data_location,
                         type_=sa.String),
            sa.bindparam('credentials', value=element.credentials,
                         type_=sa.String),
            sa.bindparam('delimiter',
                         value=element.delimiter,
                         type_=sa.String),
            sa.bindparam('ignore_header',
                         value=element.ignore_header,
                         type_=sa.Integer)
        ),
        **kw
    )


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
