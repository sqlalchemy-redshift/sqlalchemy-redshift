from setuptools import setup

setup(
    name='redshift-sqlalchemy',
    version='0.2',
    description='Amazon Redshift Dialect for sqlalchemy',
    long_description=open("README.rst").read(),
    author='Matt George',
    author_email='mgeorge@gmail.com',
    license="MIT",
    url='https://github.com/binarydud/redshift_sqlalchemy',
    packages=['redshift_sqlalchemy'],
    install_requires=['psycopg2 == 2.5', 'SQLAlchemy==0.8.0b2'],
    include_package_data=True,
    zip_safe=False,
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
    }
)

