import sqlalchemy as sa

from sqlalchemy.ext import declarative


Base = declarative.declarative_base()


class Basic(Base):
    __tablename__ = 'basic'
    name = sa.Column(
        sa.Unicode(64), primary_key=True,
        info={'distkey': True, 'sortkey': True, 'encode': 'lzo'}
    )


class IntrospectionUnique(Base):
    __tablename__ = 'test_introspecting_unique_constraint'
    col1 = sa.Column(sa.Integer(), primary_key=True)
    col2 = sa.Column(sa.Integer())
    __table_args__ = (
        sa.UniqueConstraint(col1, col2),
    )
