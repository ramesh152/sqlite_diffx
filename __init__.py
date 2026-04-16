"""
sqlite-diffx — Git-like diff for SQLite databases.

Quick start::

    from sqlite_diffx import compare

    diff = compare("old.sqlite", "new.sqlite")
    print(diff.summary())
"""
from sqlite_diffx.api.public import DiffResult, compare

__all__ = ["compare", "DiffResult"]
__version__ = "0.1.0"
