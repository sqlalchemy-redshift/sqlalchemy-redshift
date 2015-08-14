from setuptools import setup

readme = open('README.rst').read()
history = open('CHANGES.rst').read().replace('.. :changelog:', '')

setup(
    name='sqlalchemy-redshift',
    version='0.1.3.dev0',
    description='Amazon Redshift Dialect for sqlalchemy',
    long_description=readme + '\n\n' + history,
    author='Matt George',
    author_email='mgeorge@gmail.com',
    maintainer='Thomas Grainger',
    maintainer_email='sqlalchemy-redshift@graingert.co.uk',
    license="MIT",
    url='https://github.com/graingert/redshift_sqlalchemy',
    packages=['redshift_sqlalchemy'],
    install_requires=['SQLAlchemy>=0.8.0'],
    classifiers=[
        "Development Status :: 4 - Beta",
        "Environment :: Console",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python",
    ],
    entry_points={
        'sqlalchemy.dialects': [
            'redshift = redshift_sqlalchemy.dialect:PyscopgRedshiftDialect',
            'redshift.psycopg2 = redshift_sqlalchemy.dialect:PyscopgRedshiftDialect',
            'redshift.psycopg2cffi = redshift_sqlalchemy.dialect:PyscopgCFFIRedshiftDialect',
            'redshift.pg8000 = redshift_sqlalchemy.dialect:Pg8000RedshiftDialect',
            'redshift.pypostgresql = redshift_sqlalchemy.dialect:PypostgresqlRedshiftDialect',
            'redshift.zxjdbc = redshift_sqlalchemy.dialect:ZxjdbcRedshiftDialect',
        ],
    }
)

