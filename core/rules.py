"""
rules.py — Breaking change detection.

Analyses a RawDiff and classifies changes by severity:
  BREAKING  — will definitely break existing consumers (DROP TABLE, DROP COLUMN)
  WARNING   — risky but may not break everything (type changes, constraint tightening)

Pure logic: no I/O, no database access.
"""
from __future__ import annotations

from .schema import BreakingChange, RawDiff

_BREAKING = "BREAKING"
_WARNING = "WARNING"


class BreakingChangeDetector:
    """
    Scans a RawDiff for breaking and risky changes.

    Usage::

        detector = BreakingChangeDetector()
        breaking = detector.detect(raw_diff)
        hard_breaks = [b for b in breaking if b.severity == "BREAKING"]
    """

    def detect(self, diff: RawDiff) -> list[BreakingChange]:
        """
        Return all breaking and risky changes found in *diff*.

        Results are ordered: removed tables, then removed columns, then
        type changes, then NOT NULL tightening.
        """
        results: list[BreakingChange] = []
        results.extend(self._check_removed_tables(diff))
        results.extend(self._check_removed_columns(diff))
        results.extend(self._check_type_changes(diff))
        results.extend(self._check_notnull_tightened(diff))
        return results

    # ------------------------------------------------------------------
    # Private rule implementations
    # ------------------------------------------------------------------

    def _check_removed_tables(self, diff: RawDiff) -> list[BreakingChange]:
        return [
            BreakingChange(
                table=table,
                change_type="DROP_TABLE",
                description=f"Table '{table}' was removed",
                severity=_BREAKING,
            )
            for table in diff.schema.removed_tables
        ]

    def _check_removed_columns(self, diff: RawDiff) -> list[BreakingChange]:
        results: list[BreakingChange] = []
        for table_name, table_diff in diff.schema.modified_tables.items():
            for col in table_diff.removed_columns:
                results.append(
                    BreakingChange(
                        table=table_name,
                        change_type="DROP_COLUMN",
                        description=(
                            f"Column '{col.name}' was removed from table '{table_name}'"
                        ),
                        severity=_BREAKING,
                        column=col.name,
                    )
                )
        return results

    def _check_type_changes(self, diff: RawDiff) -> list[BreakingChange]:
        results: list[BreakingChange] = []
        for table_name, table_diff in diff.schema.modified_tables.items():
            for col_name, change in table_diff.modified_columns.items():
                if change.type_changed:
                    results.append(
                        BreakingChange(
                            table=table_name,
                            change_type="TYPE_CHANGE",
                            description=(
                                f"Column '{table_name}.{col_name}' type changed "
                                f"from '{change.old_type}' to '{change.new_type}' (risky)"
                            ),
                            severity=_WARNING,
                            column=col_name,
                        )
                    )
        return results

    def _check_notnull_tightened(self, diff: RawDiff) -> list[BreakingChange]:
        """Detects columns where a NOT NULL constraint was added (tightening)."""
        results: list[BreakingChange] = []
        for table_name, table_diff in diff.schema.modified_tables.items():
            for col_name, change in table_diff.modified_columns.items():
                notnull_tightened = (not change.old_notnull) and change.new_notnull
                if notnull_tightened:
                    results.append(
                        BreakingChange(
                            table=table_name,
                            change_type="NOTNULL_TIGHTENED",
                            description=(
                                f"Column '{table_name}.{col_name}' became NOT NULL "
                                f"(existing NULL values would fail)"
                            ),
                            severity=_WARNING,
                            column=col_name,
                        )
                    )
        return results
