from .read_snowflake import read_snowflake
from ._version import get_versions

__all__ = ['read_snowflake']

__version__ = get_versions()['version']
del get_versions
