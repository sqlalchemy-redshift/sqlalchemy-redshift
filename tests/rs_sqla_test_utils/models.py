import sqlalchemy as sa

from sqlalchemy import event
from sqlalchemy.ext import declarative
from sqlalchemy.schema import CreateSchema


Base = declarative.declarative_base()
event.listen(Base.metadata, 'before_create', CreateSchema('other_schema'))


class Basic(Base):
    __tablename__ = 'basic'
    name = sa.Column(
        sa.Unicode(64), primary_key=True,
        info={'distkey': True, 'sortkey': True, 'encode': 'lzo'}
    )


class BasicInOtherSchema(Base):
    __tablename__ = 'basic'
    __table_args__ = (
        {'schema': 'other_schema',
         'redshift_diststyle': 'KEY',
         'redshift_distkey': 'col1',
         'redshift_sortkey': 'col1'}
    )
    col1 = sa.Column(sa.Integer(), primary_key=True)


class ReflectionDistKey(Base):
    __tablename__ = 'reflection_distkey'
    col1 = sa.Column(sa.Integer(), primary_key=True)
    col2 = sa.Column(sa.Integer())
    __table_args__ = (
        {'redshift_diststyle': 'KEY',
         'redshift_distkey': 'col1'}
    )


class ReflectionSortKey(Base):
    __tablename__ = 'reflection_sortkey'
    col1 = sa.Column(sa.Integer(), primary_key=True)
    col2 = sa.Column(sa.Integer())
    __table_args__ = (
        {'redshift_diststyle': 'EVEN',
         'redshift_sortkey': ('col1', 'col2')}
    )


class ReflectionInterleavedSortKey(Base):
    __tablename__ = 'reflection_interleaved_sortkey'
    col1 = sa.Column(sa.Integer(), primary_key=True)
    col2 = sa.Column(sa.Integer())
    __table_args__ = (
        {'redshift_diststyle': 'EVEN',
         'redshift_interleaved_sortkey': (col1, col2)}
    )


class ReflectionSortKeyDistKeyWithSpaces(Base):
    __tablename__ = 'sort_key_with_spaces'
    col1 = sa.Column('col with spaces', sa.Integer(), nullable=False)
    __table_args__ = {
        'redshift_diststyle': 'KEY',
        'redshift_sortkey': 'col with spaces',
        'redshift_distkey': 'col with spaces',
    }
    __mapper_args__ = {
        'primary_key': [col1],
    }


class ReflectionUniqueConstraint(Base):
    __tablename__ = 'reflection_unique_constraint'
    col1 = sa.Column(sa.Integer(), primary_key=True)
    col2 = sa.Column(sa.Integer())
    __table_args__ = (
        sa.UniqueConstraint(col1, col2),
        {'redshift_diststyle': 'EVEN'}
    )


class ReflectionPrimaryKeyConstraint(Base):
    __tablename__ = 'reflection_pk_constraint'
    col1 = sa.Column(sa.Integer())
    col2 = sa.Column(sa.Integer())
    __table_args__ = (
        sa.PrimaryKeyConstraint(col1, col2),
        {'redshift_diststyle': 'EVEN'}
    )


class ReflectionForeignKeyConstraint(Base):
    __tablename__ = 'reflection_fk_constraint'
    col1 = sa.Column(sa.Integer(),
                     sa.ForeignKey('reflection_unique_constraint.col1'),
                     primary_key=True)
    col2 = sa.Column(sa.Integer())
    __table_args__ = (
        {'redshift_diststyle': 'EVEN'}
    )


class ReflectionCompositeForeignKeyConstraint(Base):
    __tablename__ = 'reflection_composite_fk_constraint'
    id = sa.Column(sa.Integer(),
                   primary_key=True)
    col1 = sa.Column(sa.Integer())
    col2 = sa.Column(sa.Integer())
    __table_args__ = (
        sa.ForeignKeyConstraint(
            ['col1', 'col2'],
            ['reflection_pk_constraint.col1', 'reflection_pk_constraint.col2']
        ),
        {'redshift_diststyle': 'EVEN'}
    )


class ReflectionDefaultValue(Base):
    __tablename__ = 'reflection_default_value'
    col1 = sa.Column(sa.Integer(), primary_key=True)
    col2 = sa.Column(sa.Integer(), server_default=sa.text('5'))
    __table_args__ = (
        {'redshift_diststyle': 'EVEN'}
    )


class ReflectionIdentity(Base):
    __tablename__ = 'reflection_identity'
    col1 = sa.Column(sa.Integer(), primary_key=True)
    col2 = sa.Column(sa.Integer(), info={'identity': (1, 3)})
    col3 = sa.Column(sa.Integer())
    __table_args__ = (
        {'redshift_diststyle': 'EVEN'}
    )


class ReflectionDelimitedIdentifiers1(Base):
    __tablename__ = 'group'
    col1 = sa.Column('this "is it"', sa.Integer(), primary_key=True)
    col2 = sa.Column('and this also', sa.Integer())
    __table_args__ = (
        {'redshift_diststyle': 'EVEN'}
    )


class ReflectionDelimitedIdentifiers2(Base):
    __tablename__ = 'column'
    col1 = sa.Column('excellent! & column', sa.Integer(), primary_key=True)
    # # TODO: Upstream fix to allow ForeignKey definition to work.
    # # Currently gives sqlalchemy.exc.ArgumentError:
    # # Can't create ForeignKeyConstraint on table 'column':
    # # no column named '"most @exce.llent "' is present.
    # sa.ForeignKey(ReflectionDelimitedIdentifiers1.col1)
    col2 = sa.Column('most @exce.llent ', sa.Integer())
    __table_args__ = (
        {'redshift_diststyle': 'EVEN'}
    )


class Referenced(Base):
    __tablename__ = 'referenced'
    id = sa.Column(
        sa.Integer(), primary_key=True, nullable=False,
        info={
            'identity': (1, 1),
        }
    )
    __table_args__ = {
        'redshift_diststyle': 'EVEN',
        'schema': 'other_schema',
    }


class Referencing(Base):
    __tablename__ = 'referencing'
    referenced_table_id = sa.Column(
        sa.Integer(), sa.ForeignKey(Referenced.id), primary_key=True,
        nullable=False,
    )
    __table_args__ = {
        'redshift_diststyle': 'EVEN',
        'schema': 'other_schema',
    }


class PostfetchExample(Base):
    __tablename__ = 'postfetch'

    id = sa.Column(sa.Integer, primary_key=True, info={'identity': (0, 1)})
    some_int = sa.Column(sa.Integer)
