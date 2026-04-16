"""
introspector.py — Extract DatabaseSchema from a live SQLite connection.

No comparison logic here — only extraction and normalization.
"""
from __future__ import annotations

import sqlite3

from .schema import ColumnDef, DatabaseSchema, TableDef


class SchemaIntrospector:
    """
    Reads the schema of a SQLite database and returns a DatabaseSchema.

    Usage::

        with open_db("mydb.sqlite") as conn:
            schema = SchemaIntrospector(conn).extract()
    """

    def __init__(self, conn: sqlite3.Connection) -> None:
        self._conn = conn

    def extract(self) -> DatabaseSchema:
        """Return the complete DatabaseSchema for all user-defined tables."""
        tables = {
            name: self._get_table_def(name)
            for name in self._get_table_names()
        }
        return DatabaseSchema(tables=tables)

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _get_table_names(self) -> list[str]:
        """
        Return a sorted list of user-defined table names.

        SQLite internal tables (prefixed with "sqlite_") are excluded.
        Sorting ensures deterministic output regardless of creation order.
        """
        rows = self._conn.execute(
            "SELECT name FROM sqlite_master "
            "WHERE type = 'table' AND name NOT LIKE 'sqlite_%' "
            "ORDER BY name"
        ).fetchall()
        return [row[0] for row in rows]

    def _get_table_def(self, table_name: str) -> TableDef:
        """
        Run PRAGMA table_info for *table_name* and return a TableDef.

        Note: PRAGMA statements do not accept ? parameter binding in SQLite.
        We use double-quote escaping to safely handle table names with special
        characters (spaces, reserved words, etc.).
        """
        safe_name = table_name.replace('"', '""')
        rows = self._conn.execute(
            f'PRAGMA table_info("{safe_name}")'
        ).fetchall()

        columns: dict[str, ColumnDef] = {}
        for row in rows:
            col = self._build_column_def(row)
            columns[col.name] = col

        return TableDef(name=table_name, columns=columns)

    def _build_column_def(self, pragma_row: sqlite3.Row) -> ColumnDef:
        """
        Convert a single PRAGMA table_info row to a ColumnDef.

        PRAGMA table_info row layout:
            (cid, name, type, notnull, dflt_value, pk)

        - cid        — column index (ignored)
        - name       — column name
        - type       — type affinity string; empty string if no type declared
        - notnull    — 1 if NOT NULL, 0 otherwise
        - dflt_value — default value as raw string, or None
        - pk         — 0 if not in PK; 1-indexed position in PK otherwise
        """
        raw_type: str = pragma_row[2] or ""
        # SQLite allows columns without a declared type; default affinity is TEXT.
        col_type = raw_type if raw_type else "TEXT"

        return ColumnDef(
            name=pragma_row[1],
            type=col_type,
            notnull=bool(pragma_row[3]),
            default=pragma_row[4],  # None or raw string e.g. "'active'", "0"
            pk=int(pragma_row[5]),
        )
