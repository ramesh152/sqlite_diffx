"""Tests for DataDiffer."""
from __future__ import annotations

import sqlite3

import pytest

from sqlite_diffx.core.data_diff import DataDiffer
from sqlite_diffx.utils.db import open_db


def _differ(path1, path2):
    with open_db(path1) as c1, open_db(path2) as c2:
        return DataDiffer(c1, c2).diff_table("users")


def _differ_ctx(path1, path2):
    """Return open connections for more complex tests — caller must close."""
    return open_db(path1), open_db(path2)


# ------------------------------------------------------------------
# PK-based diff
# ------------------------------------------------------------------


def test_inserted_rows_counted(make_db):
    db1 = make_db("a.db", {"users": [("id", "INTEGER", "PRIMARY KEY"), ("name", "TEXT")]},
                  rows={"users": [(1, "Alice"), (2, "Bob")]})
    db2 = make_db("b.db", {"users": [("id", "INTEGER", "PRIMARY KEY"), ("name", "TEXT")]},
                  rows={"users": [(1, "Alice"), (2, "Bob"), (3, "Carol")]})
    result = _differ(db1, db2)
    assert result.inserted_count == 1
    assert result.deleted_count == 0
    assert result.updated_count == 0
    assert result.strategy == "PK_BASED"


def test_deleted_rows_counted(make_db):
    db1 = make_db("a.db", {"users": [("id", "INTEGER", "PRIMARY KEY"), ("name", "TEXT")]},
                  rows={"users": [(1, "Alice"), (2, "Bob"), (3, "Carol")]})
    db2 = make_db("b.db", {"users": [("id", "INTEGER", "PRIMARY KEY"), ("name", "TEXT")]},
                  rows={"users": [(1, "Alice"), (2, "Bob")]})
    result = _differ(db1, db2)
    assert result.inserted_count == 0
    assert result.deleted_count == 1


def test_updated_rows_counted(make_db):
    db1 = make_db("a.db", {"users": [("id", "INTEGER", "PRIMARY KEY"), ("name", "TEXT")]},
                  rows={"users": [(1, "Alice"), (2, "Bob")]})
    db2 = make_db("b.db", {"users": [("id", "INTEGER", "PRIMARY KEY"), ("name", "TEXT")]},
                  rows={"users": [(1, "Alice"), (2, "Bobby")]})  # Bob → Bobby
    result = _differ(db1, db2)
    assert result.updated_count == 1
    assert result.inserted_count == 0
    assert result.deleted_count == 0


def test_identical_tables_produce_zero_diff(make_db):
    db = make_db("same.db", {"users": [("id", "INTEGER", "PRIMARY KEY"), ("name", "TEXT")]},
                 rows={"users": [(1, "Alice")]})
    result = _differ(db, db)
    assert result.inserted_count == 0
    assert result.deleted_count == 0
    assert result.updated_count == 0
    assert not result.has_changes


def test_empty_tables_produce_zero_counts(make_db):
    db = make_db("empty_users.db", {"users": [("id", "INTEGER", "PRIMARY KEY"), ("name", "TEXT")]})
    result = _differ(db, db)
    assert result.inserted_count == 0
    assert result.deleted_count == 0
    assert result.updated_count == 0


# ------------------------------------------------------------------
# COUNT_ONLY fallback (no PK)
# ------------------------------------------------------------------


def test_no_pk_table_uses_count_only_strategy(make_db):
    db1 = make_db("a.db", {"logs": [("msg", "TEXT"), ("ts", "TEXT")]},
                  rows={"logs": [("hello", "2024")]})
    db2 = make_db("b.db", {"logs": [("msg", "TEXT"), ("ts", "TEXT")]},
                  rows={"logs": [("hello", "2024"), ("world", "2025")]})
    with open_db(db1) as c1, open_db(db2) as c2:
        result = DataDiffer(c1, c2).diff_table("logs")
    assert result.strategy == "COUNT_ONLY"
    assert result.inserted_count == 1


def test_count_only_deleted(make_db):
    db1 = make_db("a.db", {"logs": [("msg", "TEXT")]},
                  rows={"logs": [("a",), ("b",), ("c",)]})
    db2 = make_db("b.db", {"logs": [("msg", "TEXT")]},
                  rows={"logs": [("a",)]})
    with open_db(db1) as c1, open_db(db2) as c2:
        result = DataDiffer(c1, c2).diff_table("logs")
    assert result.strategy == "COUNT_ONLY"
    assert result.deleted_count == 2


def test_count_only_no_change(make_db):
    db = make_db("x.db", {"logs": [("msg", "TEXT")]}, rows={"logs": [("a",), ("b",)]})
    with open_db(db) as c1, open_db(db) as c2:
        result = DataDiffer(c1, c2).diff_table("logs")
    assert not result.has_changes


# ------------------------------------------------------------------
# Composite PK
# ------------------------------------------------------------------


def test_composite_pk_diff(make_db):
    schema = {
        "order_items": (
            "CREATE TABLE order_items ("
            "order_id INTEGER, item_id INTEGER, qty INTEGER, "
            "PRIMARY KEY (order_id, item_id))"
        )
    }
    db1 = make_db("oi1.db", schema, rows={"order_items": [(1, 1, 5), (1, 2, 3)]})
    db2 = make_db("oi2.db", schema, rows={"order_items": [(1, 1, 5), (1, 2, 10), (2, 1, 7)]})
    with open_db(db1) as c1, open_db(db2) as c2:
        result = DataDiffer(c1, c2).diff_table("order_items")
    assert result.strategy == "PK_BASED"
    assert result.inserted_count == 1   # (2,1) is new
    assert result.updated_count == 1    # (1,2) qty changed
    assert result.deleted_count == 0


# ------------------------------------------------------------------
# diff_all_common_tables
# ------------------------------------------------------------------


def test_diff_all_common_tables(make_db):
    schema = {"users": [("id", "INTEGER", "PRIMARY KEY"), ("name", "TEXT")]}
    db1 = make_db("m1.db", schema, rows={"users": [(1, "A")]})
    db2 = make_db("m2.db", schema, rows={"users": [(1, "A"), (2, "B")]})
    with open_db(db1) as c1, open_db(db2) as c2:
        results = DataDiffer(c1, c2).diff_all_common_tables(["users"])
    assert "users" in results
    assert results["users"].inserted_count == 1
