redshift_sqlalchemy
===================

Amazon Redshift dialect for SQLAlchemy.

.. image:: https://travis-ci.org/sqlalchemy-redshift/sqlalchemy-redshift.png?branch=master

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

See the docstring for `RedshiftDDLCompiler` in
`dialect.py <redshift_sqlalchemy/dialect.py>`_ for more detail.
