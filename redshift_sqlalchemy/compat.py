import sys


py3k = sys.version_info >= (3, 0)

if py3k:
    string_types = (str,)
else:
    string_types = (basestring,)  # noqa
