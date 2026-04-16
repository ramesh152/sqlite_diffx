"""
schema.py — All data model dataclasses for sqlite-diffx.

This is the dependency root: every other module imports from here.
No logic, no I/O — pure data definitions.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional


# ---------------------------------------------------------------------------
# Raw schema models (extracted from SQLite)
# ---------------------------------------------------------------------------


@dataclass
class ColumnDef:
    """Represents a single column as extracted from PRAGMA table_info."""

    name: str
    type: str  # Raw SQLite type string, e.g. "INTEGER", "VARCHAR(255)", "TEXT"
    notnull: bool
    default: Optional[str]  # Raw dflt_value string from PRAGMA, or None
    pk: int  # 0 = not PK; 1+ = 1-indexed position in (composite) PK


@dataclass
class TableDef:
    """Represents a table and its columns, in definition order."""

    name: str
    columns: dict[str, ColumnDef]  # key = column name, preserves insertion order


@dataclass
class DatabaseSchema:
    """Top-level schema: all user-defined tables in a database."""

    tables: dict[str, TableDef]  # key = table name


# ---------------------------------------------------------------------------
# Diff result models
# ---------------------------------------------------------------------------


@dataclass
class ColumnChange:
    """Describes what changed about a column that exists in both schemas."""

    old_type: str
    new_type: str
    old_notnull: bool = False
    new_notnull: bool = False
    old_default: Optional[str] = None
    new_default: Optional[str] = None

    @property
    def type_changed(self) -> bool:
        return self.old_type.upper() != self.new_type.upper()

    @property
    def notnull_changed(self) -> bool:
        return self.old_notnull != self.new_notnull

    @property
    def default_changed(self) -> bool:
        return self.old_default != self.new_default

    @property
    def is_risky(self) -> bool:
        """True if type changed OR NOT NULL constraint was tightened."""
        notnull_tightened = (not self.old_notnull) and self.new_notnull
        return self.type_changed or notnull_tightened


@dataclass
class TableDiff:
    """Describes all column-level changes for a single table."""

    table_name: str
    added_columns: list[ColumnDef] = field(default_factory=list)
    removed_columns: list[ColumnDef] = field(default_factory=list)
    modified_columns: dict[str, ColumnChange] = field(default_factory=dict)  # key = col name


@dataclass
class SchemaDiff:
    """Top-level schema diff: table additions, removals, and modifications."""

    added_tables: list[str] = field(default_factory=list)
    removed_tables: list[str] = field(default_factory=list)
    modified_tables: dict[str, TableDiff] = field(default_factory=dict)  # key = table name


@dataclass
class DataDiffResult:
    """Row-level diff counts for a single table."""

    table_name: str
    inserted_count: int = 0
    deleted_count: int = 0
    updated_count: int = 0
    strategy: str = "PK_BASED"  # "PK_BASED" or "COUNT_ONLY"

    @property
    def has_changes(self) -> bool:
        return self.inserted_count != 0 or self.deleted_count != 0 or self.updated_count != 0


@dataclass
class BreakingChange:
    """A single breaking or risky change detected in the diff."""

    table: str
    change_type: str  # "DROP_TABLE" | "DROP_COLUMN" | "TYPE_CHANGE" | "NOTNULL_TIGHTENED"
    description: str
    severity: str = "BREAKING"  # "BREAKING" or "WARNING"
    column: Optional[str] = None  # populated for column-level changes


@dataclass
class RawDiff:
    """
    Internal diff result — the full picture before public API wrapping.

    Produced by DiffEngine.compare(), then enriched by BreakingChangeDetector
    and DataDiffer before being handed to the public DiffResult wrapper.
    """

    schema: SchemaDiff = field(default_factory=SchemaDiff)
    data: dict[str, DataDiffResult] = field(default_factory=dict)  # key = table name
    breaking_changes: list[BreakingChange] = field(default_factory=list)

    @property
    def is_empty(self) -> bool:
        """True when there are no schema changes and no data changes."""
        sd = self.schema
        no_schema = (
            not sd.added_tables
            and not sd.removed_tables
            and not sd.modified_tables
        )
        no_data = not any(r.has_changes for r in self.data.values())
        return no_schema and no_data
