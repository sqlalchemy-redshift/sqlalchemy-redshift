sqlalchemy-redshift
===================

Amazon Redshift dialect for SQLAlchemy.

.. image:: https://travis-ci.org/sqlalchemy-redshift/sqlalchemy-redshift.png?branch=master
   :target: https://travis-ci.org/sqlalchemy-redshift/sqlalchemy-redshift
   :alt: Travis CI build status

Installation
------------

The package is available on PyPI::

    pip install sqlalchemy-redshift

Usage
-----
The DSN format is similar to that of regular Postgres::

    >>> import sqlalchemy as sa
    >>> sa.create_engine('redshift+psycopg2://username@host.amazonaws.com:5439/database')
    Engine(redshift+psycopg2://username@host.amazonaws.com:5439/database)

See the `RedshiftDDLCompiler documentation
<https://sqlalchemy-redshift.readthedocs.org/en/latest/ddl-compiler.html>`_
for details on Redshift-specific features the dialect supports.
