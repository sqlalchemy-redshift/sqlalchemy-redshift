from setuptools import setup

readme = open('README.rst').read()
history = open('CHANGES.rst').read().replace('.. :changelog:', '')

setup(
    name='sqlalchemy-redshift',
    version='0.3.0',
    description='Amazon Redshift Dialect for sqlalchemy',
    long_description=readme + '\n\n' + history,
    author='Matt George',
    author_email='mgeorge@gmail.com',
    maintainer='Thomas Grainger',
    maintainer_email='sqlalchemy-redshift@graingert.co.uk',
    license="MIT",
    url='https://github.com/graingert/redshift_sqlalchemy',
    packages=['redshift_sqlalchemy'],
    package_data={'redshift_sqlalchemy': ['redshift-ssl-ca-cert.pem']},
    install_requires=[
        'psycopg2>=2.5',
         # requires sqlalchemy.sql.base.DialectKWArgs.dialect_options, new in
         # version 0.9.2
        'SQLAlchemy>=0.9.2',
    ],
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
            'redshift = redshift_sqlalchemy.dialect:RedshiftDialect',
            'redshift.psycopg2 = redshift_sqlalchemy.dialect:RedshiftDialect',
        ]
    },
)
