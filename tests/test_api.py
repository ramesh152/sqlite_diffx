"""Integration tests for the public compare() API."""
from __future__ import annotations

import json
import subprocess
import sys

import pytest

from sqlite_diffx import compare, DiffResult


# ------------------------------------------------------------------
# Basic API contract
# ------------------------------------------------------------------


def test_compare_returns_diff_result_type(simple_db, modified_db):
    result = compare(str(simple_db), str(modified_db))
    assert isinstance(result, DiffResult)


def test_compare_identical_dbs_is_empty(simple_db):
    result = compare(str(simple_db), str(simple_db))
    assert not result.has_breaking_changes()
    assert "No differences" in result.summary()


def test_compare_identical_path_no_breaking(simple_db):
    result = compare(str(simple_db), str(simple_db))
    assert result.has_breaking_changes() is False


# ------------------------------------------------------------------
# summary()
# ------------------------------------------------------------------


def test_summary_contains_added_table(simple_db, extra_table_db):
    result = compare(str(simple_db), str(extra_table_db))
    summary = result.summary()
    assert "[+]" in summary
    assert "products" in summary


def test_summary_contains_removed_table(extra_table_db, simple_db):
    result = compare(str(extra_table_db), str(simple_db))
    assert "[-]" in result.summary()
    assert "products" in result.summary()


def test_summary_shows_added_column(simple_db, modified_db):
    result = compare(str(simple_db), str(modified_db))
    assert "age" in result.summary()


def test_summary_shows_breaking_change(extra_table_db, simple_db):
    result = compare(str(extra_table_db), str(simple_db))
    assert "!!" in result.summary()


# ------------------------------------------------------------------
# to_json()
# ------------------------------------------------------------------


def test_to_json_is_valid_json(simple_db, modified_db):
    result = compare(str(simple_db), str(modified_db))
    parsed = json.loads(result.to_json())
    assert isinstance(parsed, dict)


def test_to_json_schema_matches_spec(simple_db, modified_db):
    result = compare(str(simple_db), str(modified_db))
    parsed = json.loads(result.to_json())
    assert "schema" in parsed
    assert "data" in parsed
    assert "breaking_changes" in parsed


def test_to_json_added_tables_key(simple_db, extra_table_db):
    result = compare(str(simple_db), str(extra_table_db))
    parsed = json.loads(result.to_json())
    assert "products" in parsed["schema"]["added_tables"]


def test_to_json_breaking_changes_structure(extra_table_db, simple_db):
    result = compare(str(extra_table_db), str(simple_db))
    parsed = json.loads(result.to_json())
    bc = parsed["breaking_changes"]
    assert isinstance(bc, list)
    assert len(bc) > 0
    assert "type" in bc[0]
    assert "severity" in bc[0]


# ------------------------------------------------------------------
# to_sql()
# ------------------------------------------------------------------


def test_to_sql_returns_string(simple_db, modified_db):
    result = compare(str(simple_db), str(modified_db))
    assert isinstance(result.to_sql(), str)


def test_to_sql_contains_alter_table(simple_db, modified_db):
    result = compare(str(simple_db), str(modified_db))
    assert "ALTER TABLE" in result.to_sql()


def test_to_sql_no_drop(extra_table_db, simple_db):
    result = compare(str(extra_table_db), str(simple_db))
    sql = result.to_sql()
    non_comment_lines = [l for l in sql.splitlines() if not l.strip().startswith("--")]
    assert not any("DROP" in l for l in non_comment_lines)


# ------------------------------------------------------------------
# has_breaking_changes()
# ------------------------------------------------------------------


def test_has_breaking_changes_true_when_table_dropped(extra_table_db, simple_db):
    result = compare(str(extra_table_db), str(simple_db))
    assert result.has_breaking_changes() is True


def test_has_breaking_changes_false_when_table_added(simple_db, extra_table_db):
    result = compare(str(simple_db), str(extra_table_db))
    assert result.has_breaking_changes() is False


def test_has_breaking_changes_false_when_column_added(simple_db, modified_db):
    result = compare(str(simple_db), str(modified_db))
    assert result.has_breaking_changes() is False


# ------------------------------------------------------------------
# include_data=False
# ------------------------------------------------------------------


def test_include_data_false_skips_data(simple_db, modified_db):
    result = compare(str(simple_db), str(modified_db), include_data=False)
    assert result.raw.data == {}


# ------------------------------------------------------------------
# Error handling
# ------------------------------------------------------------------


def test_file_not_found_raises(tmp_path):
    with pytest.raises(FileNotFoundError):
        compare(str(tmp_path / "missing.db"), str(tmp_path / "missing2.db"))


def test_first_file_not_found(simple_db, tmp_path):
    with pytest.raises(FileNotFoundError):
        compare(str(tmp_path / "gone.db"), str(simple_db))


def test_second_file_not_found(simple_db, tmp_path):
    with pytest.raises(FileNotFoundError):
        compare(str(simple_db), str(tmp_path / "gone.db"))


# ------------------------------------------------------------------
# breaking_changes property
# ------------------------------------------------------------------


def test_breaking_changes_property_returns_list(simple_db, modified_db):
    result = compare(str(simple_db), str(modified_db))
    assert isinstance(result.breaking_changes, list)


# ------------------------------------------------------------------
# CLI integration (subprocess)
# ------------------------------------------------------------------


def _run_cli(*args):
    return subprocess.run(
        [sys.executable, "-m", "sqlite_diffx.cli.main", *args],
        capture_output=True,
        text=True,
    )


def test_cli_exit_code_zero_no_breaking(simple_db, modified_db):
    proc = _run_cli("diff", str(simple_db), str(modified_db), "--fail-on-breaking")
    assert proc.returncode == 0


def test_cli_exit_code_one_fail_on_breaking(extra_table_db, simple_db):
    proc = _run_cli("diff", str(extra_table_db), str(simple_db), "--fail-on-breaking")
    assert proc.returncode == 1


def test_cli_json_output_is_valid(simple_db, modified_db):
    proc = _run_cli("diff", str(simple_db), str(modified_db), "--json")
    assert proc.returncode == 0
    parsed = json.loads(proc.stdout)
    assert "schema" in parsed


def test_cli_patch_output_contains_sql(simple_db, modified_db):
    proc = _run_cli("patch", str(simple_db), str(modified_db))
    assert proc.returncode == 0
    assert "ALTER TABLE" in proc.stdout


def test_cli_missing_file_exits_2(tmp_path):
    proc = _run_cli("diff", str(tmp_path / "nope.db"), str(tmp_path / "nope2.db"))
    assert proc.returncode == 2
