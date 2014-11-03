from setuptools import setup
from setuptools.command.test import test as TestCommand
import sys

class PyTest(TestCommand):
    def run_tests(self):
        import pytest
        errno = pytest.main(self.test_args)
        sys.exit(errno)

setup(
    name='redshift-sqlalchemy',
    version='0.5',
    description='Amazon Redshift Dialect for sqlalchemy',
    long_description=open("README.rst").read(),
    author='Matt George',
    author_email='mgeorge@gmail.com',
    license="MIT",
    url='https://github.com/binarydud/redshift_sqlalchemy',
    packages=['redshift_sqlalchemy'],
    install_requires=['psycopg2>=2.5', 'SQLAlchemy>=0.8.0'],
    tests_require=['pytest>=2.5.2'],
    test_suite="tests",
    cmdclass = {'test': PyTest},
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

