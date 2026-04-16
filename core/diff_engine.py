"""
diff_engine.py — Core schema comparison logic.

Takes two DatabaseSchema objects and produces a RawDiff.
Pure logic: no I/O, no database access.
"""
from __future__ import annotations

from .schema import (
    ColumnChange,
    ColumnDef,
    DatabaseSchema,
    RawDiff,
    SchemaDiff,
    TableDef,
    TableDiff,
)


class DiffEngine:
    """
    Compares two DatabaseSchema objects and returns a RawDiff.

    The returned RawDiff contains only schema changes; data diff and breaking
    change detection are performed by separate components (DataDiffer and
    BreakingChangeDetector) before the result is wrapped in the public API.
    """

    def compare(self, schema1: DatabaseSchema, schema2: DatabaseSchema) -> RawDiff:
        """
        Compare *schema1* (old/source) with *schema2* (new/target).

        Returns:
            RawDiff with schema changes populated. data and breaking_changes
            fields are left empty — callers fill them in separately.
        """
        schema_diff = self._diff_tables(schema1, schema2)
        return RawDiff(schema=schema_diff)

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _diff_tables(
        self, schema1: DatabaseSchema, schema2: DatabaseSchema
    ) -> SchemaDiff:
        names1 = set(schema1.tables)
        names2 = set(schema2.tables)

        added_tables = sorted(names2 - names1)
        removed_tables = sorted(names1 - names2)
        common = names1 & names2

        modified_tables: dict[str, TableDiff] = {}
        for name in sorted(common):
            td = self._diff_columns(name, schema1.tables[name], schema2.tables[name])
            if td is not None:
                modified_tables[name] = td

        return SchemaDiff(
            added_tables=added_tables,
            removed_tables=removed_tables,
            modified_tables=modified_tables,
        )

    def _diff_columns(
        self,
        table_name: str,
        table1: TableDef,
        table2: TableDef,
    ) -> TableDiff | None:
        """
        Compare two versions of the same table.

        Returns None if the tables are identical (no change worth recording).
        """
        cols1 = table1.columns
        cols2 = table2.columns
        names1 = set(cols1)
        names2 = set(cols2)

        added = [cols2[n] for n in sorted(names2 - names1)]
        removed = [cols1[n] for n in sorted(names1 - names2)]

        modified: dict[str, ColumnChange] = {}
        for name in sorted(names1 & names2):
            change = self._detect_column_change(cols1[name], cols2[name])
            if change is not None:
                modified[name] = change

        if not added and not removed and not modified:
            return None

        return TableDiff(
            table_name=table_name,
            added_columns=added,
            removed_columns=removed,
            modified_columns=modified,
        )

    def _detect_column_change(
        self, old: ColumnDef, new: ColumnDef
    ) -> ColumnChange | None:
        """
        Compare two versions of the same column.

        Returns None if they are identical in all tracked fields.
        Type comparison is case-insensitive (e.g. "text" == "TEXT").
        """
        type_same = old.type.upper() == new.type.upper()
        notnull_same = old.notnull == new.notnull
        default_same = old.default == new.default

        if type_same and notnull_same and default_same:
            return None

        return ColumnChange(
            old_type=old.type,
            new_type=new.type,
            old_notnull=old.notnull,
            new_notnull=new.notnull,
            old_default=old.default,
            new_default=new.default,
        )
