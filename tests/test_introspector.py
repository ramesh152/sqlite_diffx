"""Tests for SchemaIntrospector."""
from __future__ import annotations

import sqlite3

import pytest

from sqlite_diffx.core.introspector import SchemaIntrospector
from sqlite_diffx.utils.db import open_db


def _introspect(path):
    with open_db(path) as conn:
        return SchemaIntrospector(conn).extract()


def test_extract_returns_correct_table_names(simple_db):
    schema = _introspect(simple_db)
    assert list(schema.tables.keys()) == ["users"]


def test_empty_db_returns_empty_schema(empty_db):
    schema = _introspect(empty_db)
    assert schema.tables == {}


def test_column_names_preserved(simple_db):
    schema = _introspect(simple_db)
    assert list(schema.tables["users"].columns.keys()) == ["id", "name", "email"]


def test_column_types_preserved(simple_db):
    cols = _introspect(simple_db).tables["users"].columns
    assert cols["id"].type == "INTEGER"
    assert cols["name"].type == "TEXT"
    assert cols["email"].type == "TEXT"


def test_notnull_flag(simple_db):
    cols = _introspect(simple_db).tables["users"].columns
    assert cols["name"].notnull is True
    assert cols["email"].notnull is False


def test_pk_column_detected(simple_db):
    cols = _introspect(simple_db).tables["users"].columns
    assert cols["id"].pk == 1
    assert cols["name"].pk == 0
    assert cols["email"].pk == 0


def test_no_pk_columns_have_pk_zero(no_pk_db):
    cols = _introspect(no_pk_db).tables["logs"].columns
    for col in cols.values():
        assert col.pk == 0


def test_composite_pk_columns_detected(multi_pk_db):
    cols = _introspect(multi_pk_db).tables["order_items"].columns
    assert cols["order_id"].pk == 1
    assert cols["item_id"].pk == 2
    assert cols["qty"].pk == 0


def test_sqlite_internal_tables_excluded(tmp_path):
    """sqlite_sequence (created by AUTOINCREMENT) must not appear in schema."""
    path = tmp_path / "autoincrement.db"
    conn = sqlite3.connect(str(path))
    conn.execute(
        "CREATE TABLE items (id INTEGER PRIMARY KEY AUTOINCREMENT, val TEXT)"
    )
    conn.commit()
    conn.close()

    schema = _introspect(path)
    for name in schema.tables:
        assert not name.startswith("sqlite_"), f"Internal table leaked: {name}"


def test_multiple_tables_returned(extra_table_db):
    schema = _introspect(extra_table_db)
    assert set(schema.tables.keys()) == {"users", "products"}


def test_column_without_explicit_type_normalised_to_text(tmp_path, make_db):
    """Columns declared without a type default to TEXT affinity."""
    path = make_db("notype.db", {"t": "CREATE TABLE t (x)"})
    cols = _introspect(path).tables["t"].columns
    assert cols["x"].type == "TEXT"


def test_default_value_preserved(make_db, tmp_path):
    path = make_db(
        "defaults.db",
        {"t": [("id", "INTEGER", "PRIMARY KEY"), ("status", "TEXT", "DEFAULT", "'active'")]},
    )
    cols = _introspect(path).tables["t"].columns
    assert cols["status"].default == "'active'"


def test_no_default_is_none(simple_db):
    cols = _introspect(simple_db).tables["users"].columns
    assert cols["email"].default is None
