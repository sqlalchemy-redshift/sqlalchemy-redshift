from setuptools import setup

readme = open("README.rst", encoding="utf-8").read()
history = open("CHANGES.rst", encoding="utf-8").read().replace(".. :changelog:", "")

setup(
    name="sqlalchemy-redshift",
    version="1.0.0",
    description="Amazon Redshift Dialect for sqlalchemy",
    long_description=readme + "\n\n" + history,
    long_description_content_type="text/x-rst",
    author="Matt George",
    author_email="mgeorge@gmail.com",
    maintainer="Thomas Grainger",
    maintainer_email="sqlalchemy-redshift@graingert.co.uk",
    license="MIT",
    url="https://github.com/sqlalchemy-redshift/sqlalchemy-redshift",
    packages=["sqlalchemy_redshift"],
    package_data={"sqlalchemy_redshift": ["redshift-ca-bundle.crt"]},
    python_requires=">=3.10",
    install_requires=[
        # requires sqlalchemy.sql.base.DialectKWArgs.dialect_options, new in
        # version 0.9.2
        "SQLAlchemy>=2.0.0,<3",
        "packaging",
    ],
    extras_require={
        "dev": [
            "black",
            "isort",
        ],
        "tests": [
            "pytest",
            "alembic",
            "psycopg2-binary",
            "psycopg2cffi",
        ],
    },
    classifiers=[
        "Development Status :: 4 - Beta",
        "Environment :: Console",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Programming Language :: Python :: 3.13",
        "Programming Language :: Python :: 3.14",
    ],
    entry_points={
        "sqlalchemy.dialects": [
            "redshift = sqlalchemy_redshift.dialect:RedshiftDialect_psycopg2",
            "redshift.psycopg2 = sqlalchemy_redshift.dialect:RedshiftDialect_psycopg2",
            "redshift.psycopg2cffi = sqlalchemy_redshift.dialect:RedshiftDialect_psycopg2cffi",
            "redshift.redshift_connector = sqlalchemy_redshift.dialect:RedshiftDialect_redshift_connector",
        ]
    },
)
