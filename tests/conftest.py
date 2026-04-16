"""
conftest.py — Shared pytest fixtures for sqlite-diffx tests.

All fixtures use real SQLite on-disk databases via pytest's tmp_path.
No mocking of the database layer.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Callable

import pytest


# ---------------------------------------------------------------------------
# Core factory
# ---------------------------------------------------------------------------


@pytest.fixture
def make_db(tmp_path: Path) -> Callable:
    """
    Factory fixture that creates real SQLite database files.

    Usage::

        def test_something(make_db):
            path = make_db("test.db", {
                "users": [("id", "INTEGER", "PRIMARY KEY"),
                          ("name", "TEXT", "NOT NULL")]
            }, rows={
                "users": [(1, "Alice"), (2, "Bob")]
            })

    *schema* can be:
      - A dict of {table_name: list-of-column-tuples} for simple tables.
      - A dict of {table_name: SQL-string} for tables needing complex DDL
        (e.g. composite primary keys).

    Returns the Path of the created file.
    """

    def _make(
        filename: str,
        schema: dict,
        rows: dict | None = None,
    ) -> Path:
        path = tmp_path / filename
        conn = sqlite3.connect(str(path))
        try:
            for table_name, definition in schema.items():
                if isinstance(definition, str):
                    conn.execute(definition)
                else:
                    col_defs = ", ".join(" ".join(str(p) for p in col) for col in definition)
                    conn.execute(f"CREATE TABLE {table_name} ({col_defs})")

            if rows:
                for table_name, row_list in rows.items():
                    if row_list:
                        n_cols = len(row_list[0])
                        placeholders = ", ".join("?" * n_cols)
                        conn.executemany(
                            f"INSERT INTO {table_name} VALUES ({placeholders})",
                            row_list,
                        )
            conn.commit()
        finally:
            conn.close()
        return path

    return _make


# ---------------------------------------------------------------------------
# Concrete schema fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def empty_db(make_db) -> Path:
    """A valid SQLite database with no tables."""
    return make_db("empty.db", {})


@pytest.fixture
def simple_db(make_db) -> Path:
    """users(id PK, name NOT NULL, email)"""
    return make_db(
        "simple.db",
        {
            "users": [
                ("id", "INTEGER", "PRIMARY KEY"),
                ("name", "TEXT", "NOT NULL"),
                ("email", "TEXT"),
            ]
        },
    )


@pytest.fixture
def modified_db(make_db) -> Path:
    """users(id PK, name NOT NULL, email, age) — same as simple_db + age column."""
    return make_db(
        "modified.db",
        {
            "users": [
                ("id", "INTEGER", "PRIMARY KEY"),
                ("name", "TEXT", "NOT NULL"),
                ("email", "TEXT"),
                ("age", "INTEGER"),
            ]
        },
    )


@pytest.fixture
def no_pk_db(make_db) -> Path:
    """logs(msg, ts) — no primary key."""
    return make_db(
        "no_pk.db",
        {"logs": [("msg", "TEXT"), ("ts", "TEXT")]},
    )


@pytest.fixture
def multi_pk_db(make_db) -> Path:
    """order_items with a composite primary key (order_id, item_id)."""
    return make_db(
        "multi_pk.db",
        {
            "order_items": (
                "CREATE TABLE order_items ("
                "order_id INTEGER, item_id INTEGER, qty INTEGER, "
                "PRIMARY KEY (order_id, item_id))"
            )
        },
    )


@pytest.fixture
def extra_table_db(make_db) -> Path:
    """simple_db schema + a products table."""
    return make_db(
        "extra_table.db",
        {
            "users": [
                ("id", "INTEGER", "PRIMARY KEY"),
                ("name", "TEXT", "NOT NULL"),
                ("email", "TEXT"),
            ],
            "products": [
                ("id", "INTEGER", "PRIMARY KEY"),
                ("title", "TEXT"),
            ],
        },
    )


@pytest.fixture
def type_changed_db(make_db) -> Path:
    """users where email is VARCHAR instead of TEXT."""
    return make_db(
        "type_changed.db",
        {
            "users": [
                ("id", "INTEGER", "PRIMARY KEY"),
                ("name", "TEXT", "NOT NULL"),
                ("email", "VARCHAR"),
            ]
        },
    )
