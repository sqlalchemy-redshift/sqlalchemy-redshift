sqlalchemy-redshift
===================

Amazon Redshift dialect for SQLAlchemy.

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

Three drivers are supported::

    # psycopg2 (default)
    engine = sa.create_engine('redshift+psycopg2://user:pass@host:5439/db')

    # psycopg2cffi (PyPy compatible)
    engine = sa.create_engine('redshift+psycopg2cffi://user:pass@host:5439/db')

    # redshift_connector (Amazon's native driver)
    engine = sa.create_engine('redshift+redshift_connector://user:pass@host:5439/db')

See the `RedshiftDDLCompiler documentation
<https://sqlalchemy-redshift.readthedocs.org/en/latest/ddl-compiler.html>`_
for details on Redshift-specific features the dialect supports.

Materialized Views
~~~~~~~~~~~~~~~~~~

Create and manage materialized views::

    from sqlalchemy_redshift.ddl import (
        CreateMaterializedView, DropMaterializedView, RefreshMaterializedView
    )

    create = CreateMaterializedView('my_view', selectable, distkey='id')
    refresh = RefreshMaterializedView('my_view')
    drop = DropMaterializedView('my_view', if_exists=True)

Requirements
------------

* Python 3.10+
* SQLAlchemy >= 1.4.0, < 3
* One of: ``psycopg2``, ``psycopg2-binary``, ``psycopg2cffi``, or ``redshift_connector``

Running Tests
-------------
Tests are run via tox and can be run with the following command::

    $ tox

You can also run tests directly with pytest::

    $ pip install -e .
    $ pytest tests/ --dbdriver psycopg2 -v

To run integration tests, set the following environment variables:

* REDSHIFT_HOST
* REDSHIFT_PORT
* REDSHIFT_USERNAME
* PGPASSWORD (this is the redshift instance password)
* REDSHIFT_DATABASE
* REDSHIFT_IAM_ROLE_ARN

Note that the IAM role specified will need to be associated with
the Redshift cluster and have the correct permissions to create databases
and tables as well as drop them. Practice caution when running these tests
against a production instance.

Development
-----------

Pre-commit hooks are configured for this project::

    $ pip install pre-commit
    $ pre-commit install

Continuous Integration (CI)
---------------------------

Tests are run automatically via GitHub Actions on every push and pull request.
The CI matrix tests across Python 3.10, 3.11, and 3.12 with both
SQLAlchemy 1.4 and 2.0.

Releasing
---------

To perform a release, you will need to be an admin for the project on
GitHub and on PyPI. Contact the maintainers if you need that access.

You will need to have a `~/.pypirc` with your PyPI credentials and
also the following settings::

    [zest.releaser]
    create-wheels = yes

To perform a release, run the following::

    python -m venv ~/.virtualenvs/dist
    workon dist
    pip install -U pip setuptools wheel
    pip install -U tox zest.releaser
    fullrelease  # follow prompts, use semver ish with versions.

The releaser will handle updating version data on the package and in
CHANGES.rst along with tagging the repo and uploading to PyPI.
