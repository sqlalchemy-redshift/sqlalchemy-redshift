from sqlalchemy import Column, Integer
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy_redshift.dialect import RedshiftDialect


Base = declarative_base()


class Foo(Base):
    __tablename__ = 'foo'

    id = Column(Integer, primary_key=True, info={'identity': (0, 1)})
    some_int = Column(Integer)


def test_postfetch_lastrowid_false(_redshift_database_tool):
    RedshiftDialect.postfetch_lastrowid = False

    with _redshift_database_tool.migrated_database() as database:
        redshift_engine = database['engine']
    try:
        Base.metadata.create_all(redshift_engine)
        Session = sessionmaker(bind=redshift_engine)
        session = Session()

        for i in range(5):
            session.add(Foo(some_int=i))
        foo2 = session.query(Foo).filter(Foo.some_int == 4).first()
        assert foo2.id > 0
    finally:
        Base.metadata.drop_all(redshift_engine)


def test_postfetch_lastrowid_true(_redshift_database_tool):
    RedshiftDialect.postfetch_lastrowid = True

    with _redshift_database_tool.migrated_database() as database:
        redshift_engine = database['engine']
    try:
        Base.metadata.create_all(redshift_engine)
        Session = sessionmaker(bind=redshift_engine)
        session = Session()

        for i in range(5):
            session.add(Foo(some_int=i))
        foo2 = session.query(Foo).filter(Foo.some_int == 4).first()
        assert foo2.id is None
    finally:
        Base.metadata.drop_all(redshift_engine)
