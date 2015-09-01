sqlalchemy-redshift
===================

Amazon Redshift dialect for SQLAlchemy.

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

See more in-depth examples of building Redshift schema objects
in the :doc:`ddl-compiler` docs.
There are also several Redshift-specific :doc:`commands`
implemented in the package.

Contents:

.. toctree::
   :maxdepth: 2

   ddl-compiler
   dialect
   commands

Indices and tables
==================

* :ref:`genindex`
* :ref:`search`
