from setuptools import setup

setup(
    name='redshift-sqlalchemy',
    version='0.1',
    description='Amazon Redshift Dialect for sqlalchemy',
    long_description=open("README.rst").read(),
    author='Matt George',
    author_email='mgeorge@gmail.com',
    license="MIT",
    url='https://github.com/binarydud/redshift_sqlalchemy',
    packages=['redshift_sqlalchemy'],
    install_requires=['psycopg2 >= 2.4.1'],
    include_package_data=True,
    zip_safe=False,
    classifiers=[
        "Development Status :: 3 - Alpha",
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

