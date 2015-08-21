import sqlalchemy as sa

from sqlalchemy.ext import declarative


Base = declarative.declarative_base()


class Basic(Base):
    __tablename__ = 'basic'
    name = sa.Column(
        sa.Unicode(64), primary_key=True,
        info={'distkey': True, 'sortkey': True, 'encode': 'lzo'}
    )
