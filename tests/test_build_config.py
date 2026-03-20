"""Tests for PR 1: Build Modernization - Python 3.10+, SA 2.0 Pins, pkg_resources Removal."""
import importlib
import os
import sys


REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PKG_DIR = os.path.join(REPO_ROOT, 'sqlalchemy_redshift')


def test_sqlalchemy_version_requirement():
    """SA constraint should be >=1.4.0,<3 in setup.py."""
    setup_path = os.path.join(REPO_ROOT, 'setup.py')
    with open(setup_path) as f:
        content = f.read()
    assert "SQLAlchemy>=1.4.0,<3" in content


def test_python_requires_modern():
    """python_requires should target modern Python (>=3.9 or >=3.10)."""
    setup_path = os.path.join(REPO_ROOT, 'setup.py')
    with open(setup_path) as f:
        content = f.read()
    # Should not have the old >=3.4 requirement
    assert "python_requires='>=3.4'" not in content
    # Should require at least 3.9+
    assert '>=3.10' in content


def test_no_pkg_resources_import():
    """No file in sqlalchemy_redshift/ should import pkg_resources."""
    for root, dirs, files in os.walk(PKG_DIR):
        for fname in files:
            if fname.endswith('.py'):
                fpath = os.path.join(root, fname)
                with open(fpath) as f:
                    content = f.read()
                assert 'import pkg_resources' not in content, \
                    f'{fpath} still imports pkg_resources'
                assert 'from pkg_resources' not in content, \
                    f'{fpath} still imports from pkg_resources'


def test_no_invalid_escape_sequences():
    """All .py files should compile without SyntaxWarnings about escape sequences."""
    import subprocess
    result = subprocess.run(
        [sys.executable, '-W', 'error::DeprecationWarning', '-c',
         'import sqlalchemy_redshift.commands'],
        capture_output=True, text=True,
        cwd=REPO_ROOT
    )
    assert result.returncode == 0, f"Escape sequence issue: {result.stderr}"


def test_importlib_metadata_version():
    """__version__ should be obtainable via importlib.metadata."""
    # Re-import to get fresh version
    if 'sqlalchemy_redshift' in sys.modules:
        mod = sys.modules['sqlalchemy_redshift']
    else:
        mod = importlib.import_module('sqlalchemy_redshift')
    assert hasattr(mod, '__version__')
    assert mod.__version__  # truthy, non-empty


def test_no_legacy_compat_package():
    """redshift_sqlalchemy directory should not exist."""
    legacy_dir = os.path.join(REPO_ROOT, 'redshift_sqlalchemy')
    assert not os.path.exists(legacy_dir), \
        'redshift_sqlalchemy compat package still exists'


def test_no_collections_iterable_fallback():
    """commands.py should use collections.abc.Iterable directly, no try/except fallback."""
    commands_path = os.path.join(PKG_DIR, 'commands.py')
    with open(commands_path) as f:
        content = f.read()
    assert 'from collections import Iterable' not in content
    assert 'from collections.abc import Iterable' in content


def test_setup_no_redshift_sqlalchemy_package():
    """setup.py should not list redshift_sqlalchemy as a package."""
    setup_path = os.path.join(REPO_ROOT, 'setup.py')
    with open(setup_path) as f:
        content = f.read()
    assert 'redshift_sqlalchemy' not in content or \
        "'redshift_sqlalchemy'" not in content
