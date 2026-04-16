"""
db.py — SQLite connection helpers for sqlite-diffx.

All connection boilerplate lives here. Callers never touch sqlite3 directly.
"""
from __future__ import annotations

import contextlib
import sqlite3
from pathlib import Path
from typing import Generator


def validate_db_path(path: str | Path) -> Path:
    """
    Resolve *path* to a Path and verify it exists and is a file.

    Raises:
        FileNotFoundError: If the path does not exist or is not a regular file.
    """
    resolved = Path(path)
    if not resolved.exists():
        raise FileNotFoundError(f"Database file not found: {path}")
    if not resolved.is_file():
        raise FileNotFoundError(f"Path is not a regular file: {path}")
    return resolved


@contextlib.contextmanager
def open_db(path: str | Path) -> Generator[sqlite3.Connection, None, None]:
    """
    Open a SQLite connection and yield it, closing on exit.

    Opens in read-only mode via the SQLite URI interface to prevent accidental
    writes during diff operations. ":memory:" is the only special case —
    in-memory databases do not support URI mode.

    Yields:
        sqlite3.Connection with row_factory set to sqlite3.Row.

    Raises:
        sqlite3.DatabaseError: If the file is not a valid SQLite database.
    """
    str_path = str(path)

    if str_path == ":memory:":
        conn = sqlite3.connect(":memory:")
    else:
        # Build a read-only URI. Windows paths need forward slashes.
        posix_path = Path(path).as_posix()
        uri = f"file:{posix_path}?mode=ro"
        conn = sqlite3.connect(uri, uri=True)

    conn.row_factory = sqlite3.Row
    try:
        yield conn
    finally:
        conn.close()


def get_connection(path: str | Path) -> sqlite3.Connection:
    """
    Open a persistent read-write connection (for tests / internal use only).

    Callers are responsible for closing the returned connection.
    Production diff code should use open_db() instead.
    """
    conn = sqlite3.connect(str(path))
    conn.row_factory = sqlite3.Row
    return conn
