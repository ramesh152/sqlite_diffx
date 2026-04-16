"""Tests for DiffEngine."""
from __future__ import annotations

from sqlite_diffx.core.diff_engine import DiffEngine
from sqlite_diffx.core.introspector import SchemaIntrospector
from sqlite_diffx.utils.db import open_db


def _schema(path):
    with open_db(path) as conn:
        return SchemaIntrospector(conn).extract()


def _diff(path1, path2):
    return DiffEngine().compare(_schema(path1), _schema(path2))


def test_identical_schemas_produce_empty_diff(simple_db):
    diff = _diff(simple_db, simple_db)
    assert diff.is_empty
    assert diff.schema.added_tables == []
    assert diff.schema.removed_tables == []
    assert diff.schema.modified_tables == {}


def test_empty_vs_empty(empty_db):
    diff = _diff(empty_db, empty_db)
    assert diff.is_empty


def test_added_table_detected(simple_db, extra_table_db):
    diff = _diff(simple_db, extra_table_db)
    assert "products" in diff.schema.added_tables
    assert diff.schema.removed_tables == []


def test_removed_table_detected(extra_table_db, simple_db):
    diff = _diff(extra_table_db, simple_db)
    assert "products" in diff.schema.removed_tables
    assert diff.schema.added_tables == []


def test_added_column_detected(simple_db, modified_db):
    diff = _diff(simple_db, modified_db)
    assert "users" in diff.schema.modified_tables
    td = diff.schema.modified_tables["users"]
    added_names = [c.name for c in td.added_columns]
    assert "age" in added_names
    assert td.removed_columns == []


def test_removed_column_detected(modified_db, simple_db):
    diff = _diff(modified_db, simple_db)
    assert "users" in diff.schema.modified_tables
    td = diff.schema.modified_tables["users"]
    removed_names = [c.name for c in td.removed_columns]
    assert "age" in removed_names
    assert td.added_columns == []


def test_type_change_detected(simple_db, type_changed_db):
    diff = _diff(simple_db, type_changed_db)
    assert "users" in diff.schema.modified_tables
    td = diff.schema.modified_tables["users"]
    assert "email" in td.modified_columns
    change = td.modified_columns["email"]
    assert change.old_type == "TEXT"
    assert change.new_type == "VARCHAR"
    assert change.type_changed is True


def test_empty_vs_schema_all_added(empty_db, simple_db):
    diff = _diff(empty_db, simple_db)
    assert "users" in diff.schema.added_tables
    assert diff.schema.removed_tables == []
    assert diff.schema.modified_tables == {}


def test_schema_vs_empty_all_removed(simple_db, empty_db):
    diff = _diff(simple_db, empty_db)
    assert "users" in diff.schema.removed_tables
    assert diff.schema.added_tables == []


def test_added_tables_sorted(extra_table_db, empty_db):
    diff = _diff(empty_db, extra_table_db)
    assert diff.schema.added_tables == sorted(diff.schema.added_tables)


def test_no_false_positives_on_identical_type_case(make_db):
    """Columns with same type in different case should not show as modified."""
    db1 = make_db("a.db", {"t": [("x", "text")]})
    db2 = make_db("b.db", {"t": [("x", "TEXT")]})
    diff = _diff(db1, db2)
    # Type is same when case-normalized
    assert diff.is_empty
