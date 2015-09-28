import collections
import numbers
import re

import sqlalchemy as sa
from sqlalchemy.ext import compiler as sa_compiler
from sqlalchemy.sql import expression as sa_expression


# At the time of this implementation, no specification for a session token was
# found. After looking at a few session tokens they appear to be the same as
# the aws_secret_access_key pattern, but much longer. An example token can be
# found here:
#   https://docs.aws.amazon.com/STS/latest/APIReference/API_GetSessionToken.html
# The regexs for access keys can be found here:
#     https://blogs.aws.amazon.com/security/blog/tag/key+rotation

ACCESS_KEY_ID_RE = re.compile('[A-Z0-9]{20}')
SECRET_ACCESS_KEY_RE = re.compile('[A-Za-z0-9/+=]{40}')
TOKEN_RE = re.compile('[A-Za-z0-9/+=]+')


def _process_aws_credentials(access_key_id, secret_access_key,
                             session_token=None):

    if not ACCESS_KEY_ID_RE.match(access_key_id):
        raise ValueError(
            'invalid access_key_id; does not match {pattern}'.format(
                pattern=ACCESS_KEY_ID_RE.pattern,
            )
        )
    if not SECRET_ACCESS_KEY_RE.match(secret_access_key):
        raise ValueError(
            'invalid secret_access_key_id; does not match {pattern}'.format(
                pattern=SECRET_ACCESS_KEY_RE.pattern,
            )
        )

    credentials = 'aws_access_key_id={0};aws_secret_access_key={1}'.format(
        access_key_id,
        secret_access_key,
    )

    if session_token is not None:
        if not TOKEN_RE.match(session_token):
            raise ValueError(
                'invalid session_token; does not match {pattern}'.format(
                    pattern=TOKEN_RE.pattern,
                )
            )
        credentials += ';token={0}'.format(session_token)

    return credentials


def _process_fixed_width(spec):
    return ','.join(('{0}:{1:d}'.format(col, width) for col, width in spec))


class _ExecutableClause(sa_expression.Executable,
                        sa_expression.ClauseElement):
    pass


class UnloadFromSelect(_ExecutableClause):
    """
    Prepares a Redshift unload statement to drop a query to Amazon S3
    https://docs.aws.amazon.com/redshift/latest/dg/r_UNLOAD_command_examples.html

    Parameters
    ----------
    select: sqlalchemy.sql.selectable.Selectable
        The selectable Core Table Expression query to unload from.
    data_location: str
        The Amazon S3 location where the file will be created, or a manifest
        file if the `manifest` option is used
    access_key_id: str
    secret_access_key: str
    session_token: str, optional
    manifest: bool, optional
        Boolean value denoting whether data_location is a manifest file.
    delimiter: File delimiter, optional
        defaults to '|'
    fixed_width: iterable of (str, int), optional
        List of (column name, length) pairs to control fixed-width output.
    encrypted: bool, optional
        Write to encrypted S3 key.
    gzip: bool, optional
        Create file using GZIP compression.
    add_quotes: bool, optional
        Quote fields so that fields containing the delimiter can be
        distinguished.
    null: str, optional
        Write null values as the given string. Defaults to ''.
    escape: bool, optional
        For CHAR and VARCHAR columns in delimited unload files, an escape
        character (``\\``) is placed before every occurrence of the following
        characters: ``\\r``, ``\\n``, ``\\``, the specified delimiter string.
        If `add_quotes` is specified, ``"`` and ``'`` are also escaped.
    allow_overwrite: bool, optional
        Overwrite the key at unload_location in the S3 bucket.
    parallel: bool, optional
        If disabled unload sequentially as one file.
    """

    def __init__(self, select, unload_location, access_key_id,
                 secret_access_key, session_token=None,
                 manifest=False, delimiter=None, fixed_width=None,
                 encrypted=False, gzip=False, add_quotes=False, null=None,
                 escape=False, allow_overwrite=False, parallel=True):

        if delimiter is not None and len(delimiter) != 1:
            raise ValueError(
                '"delimiter" parameter must be a single character'
            )

        credentials = _process_aws_credentials(
            access_key_id=access_key_id,
            secret_access_key=secret_access_key,
            session_token=session_token,
        )

        self.select = select
        self.unload_location = unload_location
        self.credentials = credentials
        self.manifest = manifest
        self.delimiter = delimiter
        self.fixed_width = fixed_width
        self.encrypted = encrypted
        self.gzip = gzip
        self.add_quotes = add_quotes
        self.null = null
        self.escape = escape
        self.allow_overwrite = allow_overwrite
        self.parallel = parallel


@sa_compiler.compiles(UnloadFromSelect)
def visit_unload_from_select(element, compiler, **kw):
    """Returns the actual sql query for the UnloadFromSelect class."""

    template = """
       UNLOAD (:select) TO :unload_location
       CREDENTIALS :credentials
       {manifest}
       {delimiter}
       {encrypted}
       {fixed_width}
       {gzip}
       {add_quotes}
       {null}
       {escape}
       {allow_overwrite}
       {parallel}
    """
    el = element

    qs = template.format(
        manifest='MANIFEST' if el.manifest else '',
        delimiter=(
            'DELIMITER AS :delimiter' if el.delimiter is not None else ''
        ),
        encrypted='ENCRYPTED' if el.encrypted else '',
        fixed_width='FIXEDWIDTH AS :fixed_width' if el.fixed_width else '',
        gzip='GZIP' if el.gzip else '',
        add_quotes='ADDQUOTES' if el.add_quotes else '',
        escape='ESCAPE' if el.escape else '',
        null='NULL AS :null_as' if el.null is not None else '',
        allow_overwrite='ALLOWOVERWRITE' if el.allow_overwrite else '',
        parallel='PARALLEL OFF' if not el.parallel else '',
    )

    query = sa.text(qs)

    if el.delimiter is not None:
        query = query.bindparams(sa.bindparam(
            'delimiter', value=element.delimiter, type_=sa.String,
        ))

    if el.fixed_width:
        query = query.bindparams(sa.bindparam(
            'fixed_width',
            value=_process_fixed_width(el.fixed_width),
            type_=sa.String,
        ))

    if el.null is not None:
        query = query.bindparams(sa.bindparam(
            'null_as', value=el.null, type_=sa.String
        ))

    return compiler.process(
        query.bindparams(
            sa.bindparam('credentials', value=el.credentials, type_=sa.String),
            sa.bindparam(
                'unload_location', value=el.unload_location, type_=sa.String,
            ),
            sa.bindparam(
                'select',
                value=compiler.process(
                    el.select,
                    literal_binds=True,
                ),
                type_=sa.String,
            ),
        ),
        **kw
    )


class CopyCommand(_ExecutableClause):
    """
    Prepares a Redshift COPY statement.

    Parameters
    ----------
    to : sqlalchemy.Table or iterable of sqlalchemy.ColumnElement
        The table or columns to copy data into
    data_location : str
        The Amazon S3 location from where to copy, or a manifest file if
        the `manifest` option is used
    access_key_id : str
    secret_access_key : str
    session_token : str, optional
    format : str, optional
        CSV, JSON, or AVRO. Indicates the type of file to copy from
    quote : str, optional
        Specifies the character to be used as the quote character when using
        ``format='CSV'``. The default is a double quotation mark ( ``"`` )
    delimiter : File delimiter, optional
        defaults to ``|``
    path_file : str, optional
        Specifies an Amazon S3 location to a JSONPaths file to explicitly map
        Avro or JSON data elements to columns.
        defaults to ``'auto'``
    fixed_width: iterable of (str, int), optional
        List of (column name, length) pairs to control fixed-width output.
    compression : str, optional
        GZIP, LZOP, indicates the type of compression of the file to copy
    accept_any_date : bool, optional
        Allows any date format, including invalid formats such as
        ``00/00/00 00:00:00``, to be loaded as NULL without generating an error
        defaults to False
    accept_inv_chars : str, optional
        Enables loading of data into VARCHAR columns even if the data contains
        invalid UTF-8 characters. When specified each invalid UTF-8 byte is
        replaced by the specified replacement character
    blanks_as_null : bool, optional
        Boolean value denoting whether to load VARCHAR fields with whitespace
        only values as NULL instead of whitespace
    date_format : str, optional
        Specified the date format. If you want Amazon Redshift to automatically
        recognize and convert the date format in your source data, specify
        ``'auto'``
    empty_as_null : bool, optional
        Boolean value denoting whether to load VARCHAR fields with empty
        values as NULL instead of empty string
    encoding : str, optional
        ``'UTF8'``, ``'UTF16'``, ``'UTF16LE'``, ``'UTF16BE'``. Specifies the
        encoding type of the load data
        defaults to ``'UTF8'``
    escape : bool, optional
        When this parameter is specified, the backslash character (``\``) in
        input data is treated as an escape character. The character that
        immediately follows the backslash character is loaded into the table
        as part of the current column value, even if it is a character that
        normally serves a special purpose
    explicit_ids : bool, optional
        Override the autogenerated IDENTITY column values with explicit values
        from the source data files for the tables
    fill_record : bool, optional
        Allows data files to be loaded when contiguous columns are missing at
        the end of some of the records. The missing columns are filled with
        either zero-length strings or NULLs, as appropriate for the data types
        of the columns in question.
    ignore_blank_lines : bool, optional
        Ignores blank lines that only contain a line feed in a data file and
        does not try to load them
    ignore_header : int, optional
        Integer value of number of lines to skip at the start of each file
    dangerous_null_delimiter : str, optional
        Optional string value denoting what to interpret as a NULL value from
        the file. Note that this parameter *is not properly quoted* due to a
        difference between redshift's and postgres's COPY commands
        interpretation of strings. For example, null bytes must be passed to
        redshift's ``NULL`` verbatim as ``'\\0'`` whereas postgres's ``NULL``
        accepts ``'\\x00'``.
    remove_quotes : bool, optional
        Removes surrounding quotation marks from strings in the incoming data.
        All characters within the quotation marks, including delimiters, are
        retained.
    roundec : bool, optional
        Rounds up numeric values when the scale of the input value is greater
        than the scale of the column
    time_format : str, optional
        Specified the date format. If you want Amazon Redshift to automatically
        recognize and convert the time format in your source data, specify
        ``'auto'``
    trim_blanks : bool, optional
        Removes the trailing white space characters from a VARCHAR string
    truncate_columns : bool, optional
        Truncates data in columns to the appropriate number of characters so
        that it fits the column specification
    comp_rows : int, optional
        Specifies the number of rows to be used as the sample size for
        compression analysis
    comp_update : bool, optional
        Controls whether compression encodings are automatically applied.
        If omitted or None, COPY applies automatic compression only if the
        target table is empty and all the table columns either have RAW
        encoding or no encoding.
        If True COPY applies automatic compression if the table is empty, even
        if the table columns already have encodings other than RAW.
        If False automatic compression is disabled
    max_error : int, optional
        If the load returns the ``max_error`` number of errors or greater, the
        load fails
        defaults to 100000
    no_load : bool, optional
        Checks the validity of the data file without actually loading the data
    stat_update : bool, optional
        Update statistics automatically regardless of whether the table is
        initially empty
    manifest : bool, optional
        Boolean value denoting whether data_location is a manifest file.
    """
    formats = ['CSV', 'JSON', 'AVRO']
    compression_types = ['GZIP', 'LZOP']

    def __init__(self, to, data_location, access_key_id, secret_access_key,
                 session_token=None, format='CSV', quote=None,
                 path_file='auto', delimiter=None, fixed_width=None,
                 compression=None, accept_any_date=False,
                 accept_inv_chars=None, blanks_as_null=False, date_format=None,
                 empty_as_null=False, encoding=None, escape=False,
                 explicit_ids=False, fill_record=False,
                 ignore_blank_lines=False, ignore_header=None,
                 dangerous_null_delimiter=None, remove_quotes=False,
                 roundec=False, time_format=None, trim_blanks=False,
                 truncate_columns=False, comp_rows=None, comp_update=None,
                 max_error=None, no_load=False, stat_update=None,
                 manifest=False):

        credentials = _process_aws_credentials(
            access_key_id=access_key_id,
            secret_access_key=secret_access_key,
            session_token=session_token,
        )

        if delimiter is not None and len(delimiter) != 1:
            raise ValueError('"delimiter" parameter must be a single '
                             'character')

        if ignore_header is not None:
            if not isinstance(ignore_header, numbers.Integral):
                raise TypeError(
                    '"ignore_header" parameter should be an integer'
                )

        if format not in self.formats:
            raise ValueError('"format" parameter must be one of %s' %
                             self.formats)

        if compression is not None:
            if compression not in self.compression_types:
                raise ValueError(
                    '"compression" parameter must be one of %s' %
                    self.compression_types
                )

        table = None
        columns = []
        if isinstance(to, collections.Iterable):
            for column in to:
                if table is not None and table != column.table:
                    raise ValueError(
                        'All columns must come from the same table: '
                        '%s comes from %s not %s' % (
                            column, column.table, table
                        ),
                    )
                columns.append(column)
                table = column.table
        else:
            table = to

        self.table = table
        self.columns = columns
        self.data_location = data_location
        self.credentials = credentials
        self.format = format
        self.quote = quote
        self.path_file = path_file
        self.delimiter = delimiter
        self.fixed_width = fixed_width
        self.compression = compression
        self.manifest = manifest
        self.accept_any_date = accept_any_date
        self.accept_inv_chars = accept_inv_chars
        self.blanks_as_null = blanks_as_null
        self.date_format = date_format
        self.empty_as_null = empty_as_null
        self.encoding = encoding
        self.escape = escape
        self.explicit_ids = explicit_ids
        self.fill_record = fill_record
        self.ignore_blank_lines = ignore_blank_lines
        self.ignore_header = ignore_header
        self.dangerous_null_delimiter = dangerous_null_delimiter
        self.remove_quotes = remove_quotes
        self.roundec = roundec
        self.time_format = time_format
        self.trim_blanks = trim_blanks
        self.truncate_columns = truncate_columns
        self.comp_rows = comp_rows
        self.comp_update = comp_update
        self.max_error = max_error
        self.no_load = no_load
        self.stat_update = stat_update


@sa_compiler.compiles(CopyCommand)
def visit_copy_command(element, compiler, **kw):
    """
    Returns the actual sql query for the CopyCommand class.
    """
    qs = """COPY {table}{columns} FROM :data_location
        WITH CREDENTIALS AS :credentials
        FORMAT AS {format}
        {parameters}"""
    parameters = []
    bindparams = [
        sa.bindparam(
            'data_location',
            value=element.data_location,
            type_=sa.String,
        ),
        sa.bindparam(
            'credentials',
            value=element.credentials,
            type_=sa.String,
        ),
    ]

    if element.format == 'CSV':
        format_ = 'CSV'
        if element.quote is not None:
            format_ += ' QUOTE AS :quote_character'
            bindparams.append(sa.bindparam(
                'quote_character',
                value=element.quote,
                type_=sa.String,
            ))
    elif element.format == 'JSON':
        format_ = 'JSON AS :json_option'
        bindparams.append(sa.bindparam(
            'json_option',
            value=element.path_file,
            type_=sa.String,
        ))
    elif element.format == 'AVRO':
        format_ = 'AVRO AS :avro_option'
        bindparams.append(sa.bindparam(
            'avro_option',
            value=element.path_file,
            type_=sa.String,
        ))

    if element.delimiter is not None:
        parameters.append('DELIMITER AS :delimiter_char')
        bindparams.append(sa.bindparam(
            'delimiter_char',
            value=element.delimiter,
            type_=sa.String,
        ))

    if element.fixed_width is not None:
        parameters.append('FIXEDWIDTH AS :fixedwidth_spec')
        bindparams.append(sa.bindparam(
            'fixedwidth_spec',
            value=_process_fixed_width(element.fixed_width),
            type_=sa.String,
        ))

    if element.compression in ['GZIP', 'LZOP']:
        parameters.append(element.compression)

    if element.manifest:
        parameters.append('MANIFEST')

    if element.accept_any_date:
        parameters.append('ACCEPTANYDATE')

    if element.accept_inv_chars is not None:
        parameters.append('ACCEPTINVCHARS AS :replacement_char')
        bindparams.append(sa.bindparam(
            'replacement_char',
            value=element.accept_inv_chars,
            type_=sa.String
        ))

    if element.blanks_as_null:
        parameters.append('BLANKSASNULL')

    if element.date_format is not None:
        parameters.append('DATEFORMAT AS :dateformat_string')
        bindparams.append(sa.bindparam(
            'dateformat_string',
            value=element.date_format,
            type_=sa.String,
        ))

    if element.empty_as_null:
        parameters.append('EMPTYASNULL')

    if element.encoding in ['UTF8', 'UTF16', 'UTF16LE', 'UTF16BE']:
        parameters.append('ENCODING AS ' + element.encoding)

    if element.escape:
        parameters.append('ESCAPE')

    if element.explicit_ids:
        parameters.append('EXPLICIT_IDS')

    if element.fill_record:
        parameters.append('FILLRECORD')

    if element.ignore_blank_lines:
        parameters.append('IGNOREBLANKLINES')

    if element.ignore_header is not None:
        parameters.append('IGNOREHEADER AS :number_rows')
        bindparams.append(sa.bindparam(
            'number_rows',
            value=element.ignore_header,
            type_=sa.Integer,
        ))

    if element.dangerous_null_delimiter is not None:
        parameters.append("NULL AS'%s'" % element.dangerous_null_delimiter)

    if element.remove_quotes:
        parameters.append('REMOVEQUOTES')

    if element.roundec:
        parameters.append('ROUNDEC')

    if element.time_format is not None:
        parameters.append('TIMEFORMAT AS :timeformat_string')
        bindparams.append(sa.bindparam(
            'timeformat_string',
            value=element.time_format,
            type_=sa.String,
        ))

    if element.trim_blanks:
        parameters.append('TRIMBLANKS')

    if element.truncate_columns:
        parameters.append('TRUNCATECOLUMNS')

    if element.comp_rows:
        parameters.append('COMPROWS :numrows')
        bindparams.append(sa.bindparam(
            'numrows',
            value=element.comp_rows,
            type_=sa.Integer,
        ))

    if element.comp_update:
        parameters.append('COMPUPDATE ON')
    elif element.comp_update is not None:
        parameters.append('COMPUPDATE OFF')

    if element.max_error is not None:
        parameters.append('MAXERROR AS :error_count')
        bindparams.append(sa.bindparam(
            'error_count',
            value=element.max_error,
            type_=sa.Integer,
        ))

    if element.no_load:
        parameters.append('NOLOAD')

    if element.stat_update:
        element.append('STATUPDATE ON')
    elif element.stat_update is not None:
        element.append('STATUPDATE OFF')

    columns = ' (%s)' % ', '.join(
        compiler.preparer.format_column(column) for column in element.columns
    ) if element.columns else ''

    qs = qs.format(
        table=compiler.preparer.format_table(element.table),
        columns=columns,
        format=format_,
        parameters='\n'.join(parameters)
    )

    return compiler.process(sa.text(qs).bindparams(*bindparams), **kw)
