"""
public.py — The public Python API for sqlite-diffx.

Coordinates all core modules and exposes a clean, stable surface:

    from sqlite_diffx import compare

    diff = compare("old.sqlite", "new.sqlite")
    print(diff.summary())
    print(diff.to_json())
    print(diff.to_sql())
    print(diff.has_breaking_changes())
"""
from __future__ import annotations

from pathlib import Path

from sqlite_diffx.core.data_diff import DataDiffer
from sqlite_diffx.core.diff_engine import DiffEngine
from sqlite_diffx.core.introspector import SchemaIntrospector
from sqlite_diffx.core.patch import PatchGenerator
from sqlite_diffx.core.rules import BreakingChangeDetector
from sqlite_diffx.core.schema import DatabaseSchema
from sqlite_diffx.core.schema import RawDiff as _RawDiff
from sqlite_diffx.output.formatter import format_diff
from sqlite_diffx.output.json_out import to_json
from sqlite_diffx.utils.db import open_db, validate_db_path


class DiffResult:
    """
    The public result object returned by :func:`compare`.

    Wraps the internal :class:`RawDiff` with convenience methods so callers
    never need to import internal types.
    """

    def __init__(self, raw: _RawDiff, schema2: DatabaseSchema) -> None:
        self._raw = raw
        self._schema2 = schema2

    # ------------------------------------------------------------------
    # Output methods
    # ------------------------------------------------------------------

    def summary(self) -> str:
        """Return a human-readable plain-text diff summary."""
        return format_diff(self._raw)

    def to_json(self, indent: int = 2) -> str:
        """Return the diff serialized as a JSON string."""
        return to_json(self._raw, indent=indent)

    def to_sql(self) -> str:
        """
        Return a forward-only SQL migration script.

        Generates CREATE TABLE / ALTER TABLE ADD COLUMN statements.
        DROP statements are never generated — they appear only as comments.
        """
        return PatchGenerator().generate(self._raw, self._schema2)

    # ------------------------------------------------------------------
    # Inspection helpers
    # ------------------------------------------------------------------

    def has_breaking_changes(self) -> bool:
        """
        Return True if there are any BREAKING-severity changes.

        This is the flag used by the --fail-on-breaking CI option.
        WARNING-severity changes (type changes, NOT NULL tightening) do not
        contribute to this result.
        """
        return any(
            b.severity == "BREAKING" for b in self._raw.breaking_changes
        )

    @property
    def breaking_changes(self) -> list:
        """All BreakingChange objects (BREAKING + WARNING) from the diff."""
        return self._raw.breaking_changes

    @property
    def raw(self) -> _RawDiff:
        """
        Direct access to the internal RawDiff for advanced use.

        The structure of RawDiff is considered semi-public: it is stable
        within a major version but may change between major versions.
        """
        return self._raw


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------


def compare(
    db1_path: str | Path,
    db2_path: str | Path,
    include_data: bool = True,
) -> DiffResult:
    """
    Compare two SQLite databases and return a :class:`DiffResult`.

    Args:
        db1_path:     Path to the source (old) database.
        db2_path:     Path to the target (new) database.
        include_data: When True (default), include row-level diff counts.
                      Set to False for schema-only comparison.

    Returns:
        A :class:`DiffResult` containing schema diff, optional data diff,
        and breaking change analysis.

    Raises:
        FileNotFoundError: If either path does not exist or is not a file.
        sqlite3.DatabaseError: If either file is not a valid SQLite database.

    Example::

        from sqlite_diffx import compare

        result = compare("v1.sqlite", "v2.sqlite")
        if result.has_breaking_changes():
            print("WARNING: breaking changes detected!")
        print(result.summary())
    """
    validate_db_path(db1_path)
    validate_db_path(db2_path)

    with open_db(db1_path) as conn1, open_db(db2_path) as conn2:
        schema1 = SchemaIntrospector(conn1).extract()
        schema2 = SchemaIntrospector(conn2).extract()

        raw = DiffEngine().compare(schema1, schema2)
        raw.breaking_changes = BreakingChangeDetector().detect(raw)

        if include_data:
            common_tables = sorted(
                set(schema1.tables) & set(schema2.tables)
            )
            if common_tables:
                raw.data = DataDiffer(conn1, conn2).diff_all_common_tables(
                    common_tables
                )

    return DiffResult(raw, schema2)
