redshift_sqlalchemy
===================

Amazon Redshift dialect for sqlalchemy.

.. image:: https://travis-ci.org/graingert/redshift_sqlalchemy.png?branch=master

Requirements
-------------
* psycopg2 >= 2.5
* SQLAlchemy 0.8


Usage
-----
DSN format is simpilar to that of regular postgres:

	from sqlalchemy import create_engine
	
	engine = create_engine("redshift+psycopg2://username@host.amazonaws.com:5439/database"

Notes
-----

Currently, contraints and indexes return nothing when intropecting tables. This comes from the redshift implementation of the postgresql api == 8.0

