import sqlalchemy as sa
from sqlalchemy import exc as sa_exc
from sqlalchemy.ext import compiler as sa_compiler
from sqlalchemy.sql import expression as sa_expression
from sqlalchemy.schema import DDLElement

from .dialect import RedshiftDDLCompiler

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
    table_attributes = RedshiftDDLCompiler.get_table_attributes(
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
