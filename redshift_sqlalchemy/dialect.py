from collections import defaultdict
import itertools
import re

from sqlalchemy import schema, util, exc, inspect
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


COMPOUND_SORTKEY = 'compound'
INTERLEAVED_SORTKEY = 'interleaved'


# Regex for parsing and identity constraint out of adsrc, e.g.:
#   "identity"(445178, 0, '1,1'::text)
IDENTITY_RE = re.compile(r"""
    "identity" \(
      (?P<current>-?\d+)
      ,\s
      (?P<base>-?\d+)
      ,\s
      '(?P<seed>-?\d+),(?P<step>-?\d+)'
      .*
    \)
""", re.VERBOSE)

# Regex for foreign key constraints, e.g.:
#   FOREIGN KEY(col1) REFERENCES othertable (col2)
# See https://docs.aws.amazon.com/redshift/latest/dg/r_names.html
# for a definition of valid SQL identifiers.
FK_RE = re.compile(r"""
  ^FOREIGN\ KEY \s* \(   # FOREIGN KEY, arbitrary whitespace, literal '('
    (?P<columns>         # Start a group to capture the referring columns
      (?:                # Start a non-capturing group
        \s*              # Arbitrary whitespace
        [_a-zA-Z][\w$]*  # SQL identifier
        \s*              # Arbitrary whitespace
        ,?               # There will be a colon if this isn't the last one
      )+                 # Close the non-capturing group; require at least one
    )                    # Close the 'columns' group
  \s* \)                 # Arbitrary whitespace and literal ')'
  \s* REFERENCES \s*
  (?P<referred_table>    # Start a group to capture the referred table name
    [_a-zA-Z][\w$]*      # SQL identifier
  )
  \s* \( \s*             # Literal '(' surrounded by arbitrary whitespace
    (?P<referred_column> # Start a group to capture the referred column name
      [_a-zA-Z][\w$]*    # SQL identifier
    )
  \s* \)                 # Arbitrary whitespace and literal ')'
""", re.VERBOSE)


def _get_relation_key(name, schema):
    if schema is None:
        return name
    else:
        return schema + "." + name


def _get_schema_and_relation(key):
    before_dot, dot, after_dot = key.partition('.')
    if dot:
        return (before_dot, after_dot)
    return (None, before_dot)


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
    ...     redshift_sortkey_type="interleaved",
    ... )
    >>> print(CreateTable(user).compile(engine))
    <BLANKLINE>
    CREATE TABLE "user" (
        id INTEGER NOT NULL,
        name VARCHAR,
        PRIMARY KEY (id)
    ) DISTSTYLE KEY DISTKEY (id) INTERLEAVED SORTKEY (id, name)
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
        info = table.dialect_options['redshift']
        diststyle = info.get('diststyle')
        sortkey_type = info.get('sortkey_type', COMPOUND_SORTKEY)
        if diststyle:
            diststyle = diststyle.upper()
            if diststyle not in ('EVEN', 'KEY', 'ALL'):
                raise exc.CompileError(
                    u"diststyle {0} is invalid".format(diststyle)
                )
            text += " DISTSTYLE " + diststyle

        distkey = info.get('distkey')
        if distkey:
            text += " DISTKEY ({0})".format(distkey)

        sortkey = info.get('sortkey')
        if sortkey:
            if isinstance(sortkey, str):
                keys = (sortkey,)
            else:
                keys = sortkey
            if sortkey_type == INTERLEAVED_SORTKEY:
                text += " INTERLEAVED"
            text += " SORTKEY ({0})".format(", ".join(keys))
        return text

    def get_column_specification(self, column, **kwargs):
        colspec = self.preparer.format_column(column)

        colspec += " " + self.dialect.type_compiler.process(column.type)

        default = self.get_column_default_string(column)
        if default is not None:
            m = IDENTITY_RE.match(default)
            if m:
                colspec += " IDENTITY({seed},{step})".format(m.groupdict())
            else:
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
        identity = info.get('identity')
        if identity:
            text += " IDENTITY({0},{1})".format(identity[0], identity[1])

        encode = info.get('encode')
        if encode:
            text += " ENCODE " + encode

        distkey = info.get('distkey')
        if distkey:
            text += " DISTKEY"

        sortkey = info.get('sortkey')
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
            "diststyle": None,
            "distkey": None,
            "sortkey": None,
            "sortkey_type": COMPOUND_SORTKEY,
        }),
    ]

    def __init__(self, **kwargs):
        super(RedshiftDialect, self).__init__(self, **kwargs)
        self._all_tables_and_views = None
        self._all_columns = None
        self._all_constraints = None
        self._domains = None

    @reflection.cache
    def get_table_options(self, connection, table_name, schema, **kw):
        def keyfunc(column):
            num = int(column.sortkey)
            # If sortkey is interleaved, column numbers alternate
            # negative values, so take abs.
            return abs(num)
        table_info = self._get_cached_tables(connection, table_name, schema)
        columns = self._get_cached_columns(connection, table_name, schema)
        sortkey_cols = sorted([col for col in columns if col.sortkey],
                              key=keyfunc)
        interleaved = any([int(col.sortkey) < 0 for col in sortkey_cols])
        sortkey = [col.name for col in sortkey_cols]
        sortkey_type = INTERLEAVED_SORTKEY if interleaved else COMPOUND_SORTKEY
        distkeys = [col.name for col in columns if col.distkey]
        distkey = distkeys[0] if distkeys else None
        return {
            'redshift_diststyle': table_info.diststyle,
            'redshift_distkey': distkey,
            'redshift_sortkey': sortkey,
            'redshift_sortkey_type': sortkey_type,
        }

    @reflection.cache
    def get_columns(self, connection, table_name, schema=None, **kw):
        """Return information about columns in `table_name`.

        Interface defined in :meth:`~sqlalchemy.engine.Dialect.get_columns`.
        """
        cols = self._get_cached_columns(connection, table_name, schema)
        if not self._domains:
            self._domains = self._load_domains(connection)
        domains = self._domains
        columns = []
        for col in cols:
            column_info = self._get_column_info(
                name=col.name, format_type=col.format_type,
                default=col.default, notnull=col.notnull, domains=domains,
                enums=[], schema=col.schema, encode=col.encode)
            columns.append(column_info)
        return columns

    @reflection.cache
    def get_pk_constraint(self, connection, table_name, schema=None, **kw):
        """
        Return information about the primary key constraint on `table_name`.

        Interface defined in :meth:`~sqlalchemy.engine.Dialect.get_pk_constraint`.
        """
        constraints = self._get_cached_constraints(connection, table_name,
                                                   schema)
        pk_constraints = [c for c in constraints if c.contype == 'p']
        if not pk_constraints:
            return {'constrained_columns': [], 'name': ''}
        pk_constraint = pk_constraints[0]
        # The slice here removes the 'PRIMARY KEY(' prefix and the ')' suffix
        column_text = pk_constraint.condef[13:-1]
        constrained_columns = column_text.split(', ')
        return {
            'constrained_columns': constrained_columns,
            'name': None,
        }

    @reflection.cache
    def get_foreign_keys(self, connection, table_name, schema=None, **kw):
        """
        Return information about foreign keys in `table_name`.

        Interface defined in :meth:`~sqlalchemy.engine.Dialect.get_pk_constraint`.
        """
        constraints = self._get_cached_constraints(connection, table_name,
                                                   schema)
        fk_constraints = [c for c in constraints if c.contype == 'f']
        fkeys = []
        for constraint in fk_constraints:
            m = FK_RE.match(constraint.condef)
            groups = m.groupdict()
            colstring = groups['columns']
            referred_table = groups['referred_table']
            referred_column = groups['referred_column']
            referred_schema = None
            if '.' in referred_table:
                referred_table, referred_schema = referred_table.split('.')
            constrained_columns = [c.strip() for c in colstring.split(',')]
            referred_columns = [referred_column]
            fkey_d = {
                'name': None,
                'constrained_columns': constrained_columns,
                'referred_schema': referred_schema,
                'referred_table': referred_table,
                'referred_columns': referred_columns,
            }
            fkeys.append(fkey_d)
        return fkeys

    @reflection.cache
    def get_table_names(self, connection, schema=None, **kw):
        """
        Return a list of table names for `schema`.
        """
        all_tables, _ = self._get_all_table_and_view_info(connection)
        table_names = []
        for key in all_tables.keys():
            s, t = _get_schema_and_relation(key)
            if s == schema:
                table_names.append(t)
        return table_names

    # TODO: Implement this
    # def get_temp_table_names(self, connection, schema=None, **kw):
    #     """Return a list of temporary table names on the given connection,
    #     if supported by the underlying backend.
    #     """

    #     raise NotImplementedError()

    @reflection.cache
    def get_view_names(self, connection, schema=None, **kw):
        """
        Return a list of all view names available in the database.
        """
        _, all_views = self._get_all_table_and_view_info(connection)
        view_names = []
        for key in all_views.keys():
            s, v = _get_schema_and_relation(key)
            if s == schema:
                view_names.append(v)
        return view_names

    # TODO: Implement this
    # def get_temp_view_names(self, connection, schema=None, **kw):
    #     """Return a list of temporary view names on the given connection,
    #     if supported by the underlying backend.
    #     """

    #     raise NotImplementedError()

    @reflection.cache
    def get_view_definition(self, connection, view_name, schema=None, **kw):
        """Return view definition.
        Given a :class:`.Connection`, a string
        `view_name`, and an optional string `schema`, return the view
        definition.
        """
        view = self._get_cached_views(connection, view_name, schema)
        return view.view_definition

    @reflection.cache
    def get_unique_constraints(self, connection, table_name,
                               schema=None, **kw):
        """Return information about unique constraints in `table_name`.
        """
        constraints = [c for c in
                       self._get_cached_constraints(
                           connection, table_name, schema)
                       if c.contype == 'u']
        uniques = defaultdict(lambda: defaultdict(dict))
        for row in constraints:
            uc = uniques[row.conname]
            uc["key"] = row.conkey
            uc["cols"][row.attnum] = row.attname
        return [
            {'name': None,
             'column_names': [uc["cols"][i] for i in uc["key"]]}
            for name, uc in uniques.items()
        ]

    def get_indexes(self, connection, table_name, schema, **kw):
        """
        Redshift does not use traditional indexes.
        """
        return []

    @reflection.cache
    def has_table(self, connection, table_name, schema=None):
        """Check the existence of a particular table in the database.
        """
        key = _get_relation_key(table_name, schema)
        return key in self.get_table_names(connection, schema=schema)

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
        kw = kwargs.copy()
        encode = kw.pop('encode', None)
        column_info = super(RedshiftDialect, self)._get_column_info(
            *args,
            **kw
        )
        if isinstance(column_info['type'], VARCHAR):
            if column_info['type'].length is None:
                column_info['type'] = NullType()
        if 'info' not in column_info:
            column_info['info'] = {}
        if encode and encode != 'none':
            column_info['info']['encode'] = encode
        return column_info

    def _get_cached_tables(self, connection, table_name, schema=None):
        key = _get_relation_key(table_name, schema)
        all_tables, _ = self._get_all_table_and_view_info(connection)
        return all_tables[key]

    def _get_cached_views(self, connection, view_name, schema=None):
        key = _get_relation_key(view_name, schema)
        _, all_views = self._get_all_table_and_view_info(connection)
        return all_views[key]

    def _get_cached_columns(self, connection, table_name, schema=None):
        key = _get_relation_key(table_name, schema)
        all_columns = self._get_all_column_info(connection)
        return all_columns[key]

    def _get_cached_constraints(self, connection, table_name, schema=None):
        key = _get_relation_key(table_name, schema)
        all_constraints = self._get_all_constraint_info(connection)
        return all_constraints[key]

    def _get_all_table_and_view_info(self, connection):
        if self._all_tables_and_views:
            return self._all_tables_and_views
        result = connection.execute("""
        SELECT
          c.relkind,
          n.oid as "schema_oid",
          n.nspname as "schema",
          c.oid as "rel_oid",
          c.relname,
          CASE c.reldiststyle
            WHEN 0 THEN 'EVEN' WHEN 1 THEN 'KEY' WHEN 8 THEN 'ALL' END
            AS "diststyle",
          c.relowner AS "owner_id",
          u.usename AS "owner_name",
          pg_get_viewdef(c.oid) AS "view_definition",
          pg_catalog.array_to_string(c.relacl, '\n') AS "privileges"
        FROM pg_catalog.pg_class c
             LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
             JOIN pg_catalog.pg_user u ON u.usesysid = c.relowner
        WHERE c.relkind IN ('r', 'v', 'm', 'S', 'f')
          AND n.nspname !~ '^pg_' AND pg_catalog.pg_table_is_visible(c.oid)
        ORDER BY 1, 2, 3;
        """)
        tables, views = {}, {}
        for r in result:
            schema = r.schema
            if schema == inspect(connection).default_schema_name:
                schema = None
            key = _get_relation_key(r.relname, schema)
            if r.relkind == 'r':
                tables[key] = r
            if r.relkind == 'v':
                views[key] = r
        self._all_tables_and_views = (tables, views)
        return self._all_tables_and_views

    def _get_all_column_info(self, connection):
        if self._all_columns:
            return self._all_columns
        result = connection.execute("""
        SELECT
          n.nspname as "schema",
          c.relname as "table_name",
          d.column as "name",
          encoding as "encode",
          type, distkey, sortkey, "notnull", adsrc, attnum,
          pg_catalog.format_type(att.atttypid, att.atttypmod),
          pg_catalog.pg_get_expr(ad.adbin, ad.adrelid) AS DEFAULT,
          n.oid as "schema_oid",
          c.oid as "table_oid"
        FROM pg_catalog.pg_class c
        LEFT JOIN pg_catalog.pg_namespace n
          ON n.oid = c.relnamespace
        JOIN pg_catalog.pg_table_def d
          ON (d.schemaname, d.tablename) = (n.nspname, c.relname)
        JOIN pg_catalog.pg_attribute att
          ON (att.attrelid, att.attname) = (c.oid, d.column)
        LEFT JOIN pg_catalog.pg_attrdef ad
          ON (att.attrelid, att.attnum) = (ad.adrelid, ad.adnum)
        WHERE n.nspname !~ '^pg_' AND pg_catalog.pg_table_is_visible(c.oid)
        ORDER BY n.nspname, c.relname
        """)
        all_columns = defaultdict(list)
        for col in result:
            schema = col.schema
            if schema == inspect(connection).default_schema_name:
                schema = None
            key = _get_relation_key(col.table_name, schema)
            all_columns[key].append(col)
        self._all_columns = all_columns
        return self._all_columns

    @reflection.cache
    def _get_all_constraint_info(self, connection):
        if self._all_constraints:
            return self._all_constraints
        result = connection.execute("""
        SELECT
          n.nspname as "schema",
          c.relname as "table_name",
          t.contype,
          t.conname,
          t.conkey,
          a.attnum,
          a.attname,
          pg_catalog.pg_get_constraintdef(t.oid, true) as condef,
          n.oid as "schema_oid",
          c.oid as "rel_oid"
        FROM pg_catalog.pg_class c
        LEFT JOIN pg_catalog.pg_namespace n
          ON n.oid = c.relnamespace
        JOIN pg_catalog.pg_constraint t
          ON t.conrelid = c.oid
        JOIN pg_catalog.pg_attribute a
          ON t.conrelid = a.attrelid AND a.attnum = ANY(t.conkey)
        WHERE n.nspname !~ '^pg_' AND pg_catalog.pg_table_is_visible(c.oid)
        ORDER BY n.nspname, c.relname
        """)
        all_constraints = defaultdict(list)
        for con in result:
            schema = con.schema
            if schema == inspect(connection).default_schema_name:
                schema = None
            key = _get_relation_key(con.table_name, schema)
            all_constraints[key].append(con)
        self._all_constraints = all_constraints
        return self._all_constraints


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
