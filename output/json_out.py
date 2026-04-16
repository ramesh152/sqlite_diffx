"""
json_out.py — JSON serialization for sqlite-diffx.

Converts a RawDiff to a structured JSON string.
"""
from __future__ import annotations

import json
from dataclasses import asdict

from sqlite_diffx.core.schema import RawDiff


def to_json(diff: RawDiff, indent: int = 2) -> str:
    """Return a JSON string representing *diff*."""
    return json.dumps(_serialize_diff(diff), indent=indent)


# ---------------------------------------------------------------------------
# Internal serializers
# ---------------------------------------------------------------------------


def _serialize_diff(diff: RawDiff) -> dict:
    return {
        "schema": _serialize_schema(diff),
        "data": _serialize_data(diff),
        "breaking_changes": _serialize_breaking(diff),
    }


def _serialize_schema(diff: RawDiff) -> dict:
    modified: dict = {}
    for table_name, table_diff in diff.schema.modified_tables.items():
        modified[table_name] = {
            "added_columns": [asdict(c) for c in table_diff.added_columns],
            "removed_columns": [asdict(c) for c in table_diff.removed_columns],
            "modified_columns": {
                col_name: {
                    "from": change.old_type,
                    "to": change.new_type,
                    "notnull_changed": change.notnull_changed,
                    "old_notnull": change.old_notnull,
                    "new_notnull": change.new_notnull,
                    "default_changed": change.default_changed,
                    "old_default": change.old_default,
                    "new_default": change.new_default,
                }
                for col_name, change in table_diff.modified_columns.items()
            },
        }

    return {
        "added_tables": diff.schema.added_tables,
        "removed_tables": diff.schema.removed_tables,
        "modified_tables": modified,
    }


def _serialize_data(diff: RawDiff) -> dict:
    return {
        name: {
            "inserted": r.inserted_count,
            "deleted": r.deleted_count,
            "updated": r.updated_count,
            "strategy": r.strategy,
        }
        for name, r in diff.data.items()
        if r.has_changes
    }


def _serialize_breaking(diff: RawDiff) -> list:
    return [
        {
            "table": b.table,
            "type": b.change_type,
            "description": b.description,
            "severity": b.severity,
            "column": b.column,
        }
        for b in diff.breaking_changes
    ]
