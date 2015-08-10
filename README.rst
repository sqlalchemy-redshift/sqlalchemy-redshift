redshift_sqlalchemy
===================

Amazon Redshift dialect for SQLAlchemy.

.. image:: https://travis-ci.org/graingert/redshift_sqlalchemy.png?branch=master

Requirements
-------------
* psycopg2 >= 2.5
* SQLAlchemy 0.8


Usage
-----
The DSN format is similar to that of regular Postgres:

	from sqlalchemy import create_engine
	
	engine = create_engine("redshift+psycopg2://username@host.amazonaws.com:5439/database"

Notes
-----

Currently, constraints and indexes return nothing when introspecting tables. This is because Redshift implements version 8.0 of the PostgreSQL API.

