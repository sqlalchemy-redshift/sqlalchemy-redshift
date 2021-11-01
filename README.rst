sqlalchemy-redshift
===================

Amazon Redshift dialect for SQLAlchemy.

.. image:: https://travis-ci.org/sqlalchemy-redshift/sqlalchemy-redshift.svg?branch=main
   :target: https://travis-ci.org/sqlalchemy-redshift/sqlalchemy-redshift
   :alt: Travis CI build status

Installation
------------

The package is available on PyPI::

    pip install sqlalchemy-redshift

.. warning::

    This dialect requires either ``redshift_connector`` or ``psycopg2``
    to work properly. It does not provide
    it as required, but relies on you to select the distribution you need:

    * psycopg2 - standard distribution of psycopg2, requires compilation so few system dependencies are required for it
    * psycopg2-binary - already compiled distribution (no system dependencies are required)
    * psycopg2cffi - pypy compatible version

    See `Psycopg2's binary install docs <http://initd.org/psycopg/docs/install.html#binary-install-from-pypi>`_
    for more context on choosing a distribution.

Usage
-----
The DSN format is similar to that of regular Postgres::

    >>> import sqlalchemy as sa
    >>> sa.create_engine('redshift+psycopg2://username@host.amazonaws.com:5439/database')
    Engine(redshift+psycopg2://username@host.amazonaws.com:5439/database)

See the `RedshiftDDLCompiler documentation
<https://sqlalchemy-redshift.readthedocs.org/en/latest/ddl-compiler.html>`_
for details on Redshift-specific features the dialect supports.

Releasing
---------

To perform a release, you will need to be an admin for the project on
GitHub and on PyPI. Contact the maintainers if you need that access.

You will need to have a `~/.pypirc` with your PyPI credentials and
also the following settings::

    [zest.releaser]
    create-wheels = yes

To perform a release, run the following::

    python3.6 -m venv ~/.virtualenvs/dist
    workon dist
    pip install -U pip setuptools wheel
    pip install -U tox zest.releaser
    fullrelease  # follow prompts, use semver ish with versions.

The releaser will handle updating version data on the package and in
CHANGES.rst along with tagging the repo and uploading to PyPI.
