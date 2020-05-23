import sqlalchemy as sa
from sqlalchemy import exc as sa_exc, Column
from sqlalchemy.ext import compiler as sa_compiler
from sqlalchemy.schema import DDLElement

from .compat import string_types

def _check_if_key_exists(key):
    return isinstance(key, Column) or key

def get_table_attributes(preparer,
                         diststyle=None,
                         distkey=None,
                         sortkey=None,
                         interleaved_sortkey=None):
    """
    Parse the table attributes into an acceptable string for Redshift,
    checking for valid combinations of distribution options.

    Parameters
    ----------
    preparer: RedshiftIdentifierPreparer
        TODO
    """
    # TODO fill out above
    text = ""
    has_distkey = _check_if_key_exists(distkey)
    if diststyle:
        diststyle = diststyle.upper()
        if diststyle not in ('EVEN', 'KEY', 'ALL'):
            raise sa_exc.ArgumentError(
                u"diststyle {0} is invalid".format(diststyle)
            )
        if diststyle != 'KEY' and has_distkey:
            raise sa_exc.ArgumentError(
                u"DISTSTYLE EVEN/ALL is not compatible with a DISTKEY."
            )
        text += " DISTSTYLE " + diststyle

    if has_distkey:
        if isinstance(distkey, Column):
            distkey = distkey.name
        text += " DISTKEY ({0})".format(preparer.quote(distkey))

    has_sortkey = _check_if_key_exists(sortkey)
    has_interleaved = _check_if_key_exists(interleaved_sortkey)
    if has_sortkey and has_interleaved:
        raise sa_exc.ArgumentError(
            "Parameters sortkey and interleaved_sortkey are "
            "mutually exclusive; you may not specify both."
        )
    if has_sortkey or has_interleaved:
        keys = sortkey if has_sortkey else interleaved_sortkey
        if isinstance(keys, (string_types, Column)):
            keys = [keys]
        keys = [key.name if isinstance(key, Column) else key
                for key in keys]
        if interleaved_sortkey:
            text += " INTERLEAVED"
        sortkey_string = ", ".join(preparer.quote(key)
                                   for key in keys)
        text += " SORTKEY ({0})".format(sortkey_string)
    return text


class CreateMaterializedView(DDLElement):
    """
    """
    # TODO docs above
    def __init__(self, name, selectable, backup=True, diststyle=None,
                 distkey=None, sortkey=None, interleaved_sortkey=None):
        self.name = name
        self.selectable = selectable
        self.backup = backup
        self.diststyle = diststyle
        self.distkey = distkey
        self.sortkey = sortkey
        self.interleaved_sortkey = interleaved_sortkey


@sa_compiler.compiles(CreateMaterializedView)
def compile_create_materialized_view(element, compiler, **kw):
    """
    Returns the sql query that creates the materialized view
    """

    text = """
        CREATE MATERIALIZED VIEW {name}
        BACKUP {backup}
        {table_attributes}
        AS {selectable}
    """

    # TODO check element.preparer is the right type
    table_attributes = get_table_attributes(
        element.preparer,
        diststyle=element.diststyle,
        distkey=element.distkey,
        sortkey=element.sortkey,
        interleaved_sortkey=element.interleaved_sortkey
    )
    backup = "YES" if element.backup else "NO"
    selectable = compiler.sql_compiler.process(element.selectable,
                                               literal_binds=True)

    text = text.format(
        name=element.name,
        backup=backup,
        table_attributes=table_attributes,
        selectable=selectable
    )
    return compiler.process(sa.text(text), **kw)
