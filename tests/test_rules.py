"""Tests for BreakingChangeDetector."""
from __future__ import annotations

from sqlite_diffx.core.rules import BreakingChangeDetector
from sqlite_diffx.core.schema import (
    BreakingChange,
    ColumnChange,
    ColumnDef,
    RawDiff,
    SchemaDiff,
    TableDiff,
)


def _detect(raw: RawDiff) -> list[BreakingChange]:
    return BreakingChangeDetector().detect(raw)


def _make_diff(**kwargs) -> RawDiff:
    return RawDiff(schema=SchemaDiff(**kwargs))


# ------------------------------------------------------------------
# Empty diff
# ------------------------------------------------------------------


def test_empty_diff_no_breaking_changes():
    assert _detect(RawDiff()) == []


# ------------------------------------------------------------------
# DROP TABLE
# ------------------------------------------------------------------


def test_removed_table_is_breaking():
    raw = _make_diff(removed_tables=["old_table"])
    results = _detect(raw)
    assert len(results) == 1
    assert results[0].change_type == "DROP_TABLE"
    assert results[0].severity == "BREAKING"
    assert results[0].table == "old_table"


def test_added_table_not_breaking():
    raw = _make_diff(added_tables=["new_table"])
    assert _detect(raw) == []


# ------------------------------------------------------------------
# DROP COLUMN
# ------------------------------------------------------------------


def _col(name, col_type="TEXT") -> ColumnDef:
    return ColumnDef(name=name, type=col_type, notnull=False, default=None, pk=0)


def test_removed_column_is_breaking():
    td = TableDiff(table_name="users", removed_columns=[_col("legacy")])
    raw = RawDiff(schema=SchemaDiff(modified_tables={"users": td}))
    results = _detect(raw)
    assert any(r.change_type == "DROP_COLUMN" and r.severity == "BREAKING" for r in results)


def test_added_column_not_breaking():
    td = TableDiff(table_name="users", added_columns=[_col("age")])
    raw = RawDiff(schema=SchemaDiff(modified_tables={"users": td}))
    assert _detect(raw) == []


# ------------------------------------------------------------------
# TYPE CHANGE (WARNING)
# ------------------------------------------------------------------


def test_type_change_is_warning():
    change = ColumnChange(old_type="TEXT", new_type="VARCHAR")
    td = TableDiff(table_name="users", modified_columns={"email": change})
    raw = RawDiff(schema=SchemaDiff(modified_tables={"users": td}))
    results = _detect(raw)
    assert any(r.change_type == "TYPE_CHANGE" and r.severity == "WARNING" for r in results)


def test_type_change_not_breaking():
    change = ColumnChange(old_type="TEXT", new_type="VARCHAR")
    td = TableDiff(table_name="users", modified_columns={"email": change})
    raw = RawDiff(schema=SchemaDiff(modified_tables={"users": td}))
    results = _detect(raw)
    assert not any(r.severity == "BREAKING" for r in results)


# ------------------------------------------------------------------
# NOTNULL TIGHTENED (WARNING)
# ------------------------------------------------------------------


def test_notnull_tightened_is_warning():
    change = ColumnChange(old_type="TEXT", new_type="TEXT", old_notnull=False, new_notnull=True)
    td = TableDiff(table_name="users", modified_columns={"name": change})
    raw = RawDiff(schema=SchemaDiff(modified_tables={"users": td}))
    results = _detect(raw)
    assert any(r.change_type == "NOTNULL_TIGHTENED" and r.severity == "WARNING" for r in results)


def test_notnull_loosened_not_flagged():
    change = ColumnChange(old_type="TEXT", new_type="TEXT", old_notnull=True, new_notnull=False)
    td = TableDiff(table_name="users", modified_columns={"name": change})
    raw = RawDiff(schema=SchemaDiff(modified_tables={"users": td}))
    results = _detect(raw)
    assert not any(r.change_type == "NOTNULL_TIGHTENED" for r in results)


# ------------------------------------------------------------------
# Multiple changes
# ------------------------------------------------------------------


def test_multiple_breaking_changes():
    raw = _make_diff(
        removed_tables=["t1", "t2"],
    )
    results = _detect(raw)
    assert len(results) == 2
    assert all(r.severity == "BREAKING" for r in results)


def test_has_breaking_true_for_breaking_severity():
    raw = _make_diff(removed_tables=["t"])
    results = _detect(raw)
    assert any(r.severity == "BREAKING" for r in results)


def test_column_field_populated_for_column_changes():
    td = TableDiff(table_name="users", removed_columns=[_col("x")])
    raw = RawDiff(schema=SchemaDiff(modified_tables={"users": td}))
    results = _detect(raw)
    assert results[0].column == "x"


def test_column_field_none_for_table_removal():
    raw = _make_diff(removed_tables=["t"])
    results = _detect(raw)
    assert results[0].column is None
