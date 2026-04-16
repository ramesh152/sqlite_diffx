"""
data_diff.py — Row-level diff using primary keys.

Compares the actual data in two SQLite databases, table by table.
Returns only counts (inserted / deleted / updated) — not full row diffs.

Strategy:
  PK_BASED   — used when both tables share the same PK columns.
               Accurately identifies inserted, deleted, and updated rows.
  COUNT_ONLY — fallback when a table has no PK or PKs differ between DBs.
               Only reports the net change in row count.
"""
from __future__ import annotations

import sqlite3

from .schema import DataDiffResult

# Keep well below the SQLite variable limit (32766 on SQLite 3.32+, 999 on older).
_CHUNK_SIZE = 900


class DataDiffer:
    """
    Compares row-level data in tables shared by two SQLite databases.

    Only tables present in *both* databases are diffed here; schema-only
    additions / removals are handled by DiffEngine.

    Usage::

        with open_db(db1) as c1, open_db(db2) as c2:
            results = DataDiffer(c1, c2).diff_all_common_tables(["users", "orders"])
    """

    def __init__(
        self, conn1: sqlite3.Connection, conn2: sqlite3.Connection
    ) -> None:
        self._conn1 = conn1
        self._conn2 = conn2

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def diff_all_common_tables(
        self, tables: list[str]
    ) -> dict[str, DataDiffResult]:
        """
        Diff each table in *tables*.

        *tables* should contain only names that exist in both databases.
        Returns a dict keyed by table name.
        """
        return {table: self.diff_table(table) for table in tables}

    def diff_table(self, table: str) -> DataDiffResult:
        """
        Diff a single table.

        Chooses PK_BASED strategy when both connections have identical PK
        columns; falls back to COUNT_ONLY otherwise.
        """
        pk1 = self._get_pk_columns(self._conn1, table)
        pk2 = self._get_pk_columns(self._conn2, table)

        if pk1 and pk2 and pk1 == pk2:
            return self._pk_based_diff(table, pk1)
        return self._count_based_diff(table)

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _get_pk_columns(
        self, conn: sqlite3.Connection, table: str
    ) -> list[str]:
        """
        Return ordered list of PK column names for *table*.

        Returns an empty list if the table has no declared primary key.
        Uses double-quoted table name because PRAGMA does not accept ?-binding.
        """
        safe = table.replace('"', '""')
        rows = conn.execute(f'PRAGMA table_info("{safe}")').fetchall()
        pk_rows = sorted(
            [(row[1], row[5]) for row in rows if row[5] > 0],
            key=lambda x: x[1],  # sort by pk position (1-indexed)
        )
        return [name for name, _ in pk_rows]

    def _pk_based_diff(
        self, table: str, pk_cols: list[str]
    ) -> DataDiffResult:
        """Full PK-based diff: inserted, deleted, and updated row counts."""
        pks1 = self._fetch_pk_set(self._conn1, table, pk_cols)
        pks2 = self._fetch_pk_set(self._conn2, table, pk_cols)

        inserted_pks = pks2 - pks1
        deleted_pks = pks1 - pks2
        common_pks = pks1 & pks2

        updated = 0
        if common_pks:
            rows1 = self._fetch_rows_by_pks(self._conn1, table, pk_cols, list(common_pks))
            rows2 = self._fetch_rows_by_pks(self._conn2, table, pk_cols, list(common_pks))
            updated = sum(1 for pk in common_pks if rows1.get(pk) != rows2.get(pk))

        return DataDiffResult(
            table_name=table,
            inserted_count=len(inserted_pks),
            deleted_count=len(deleted_pks),
            updated_count=updated,
            strategy="PK_BASED",
        )

    def _count_based_diff(self, table: str) -> DataDiffResult:
        """Fallback when no PK is available: compare row counts only."""
        safe = table.replace('"', '""')
        c1 = self._conn1.execute(f'SELECT COUNT(*) FROM "{safe}"').fetchone()[0]
        c2 = self._conn2.execute(f'SELECT COUNT(*) FROM "{safe}"').fetchone()[0]
        delta = c2 - c1

        if delta > 0:
            return DataDiffResult(
                table_name=table, inserted_count=delta, strategy="COUNT_ONLY"
            )
        if delta < 0:
            return DataDiffResult(
                table_name=table, deleted_count=abs(delta), strategy="COUNT_ONLY"
            )
        return DataDiffResult(table_name=table, strategy="COUNT_ONLY")

    def _fetch_pk_set(
        self,
        conn: sqlite3.Connection,
        table: str,
        pk_cols: list[str],
    ) -> set[tuple]:
        """Return a set of PK tuples for every row in *table*."""
        safe_table = table.replace('"', '""')
        col_list = ", ".join(f'"{c.replace(chr(34), chr(34)+chr(34))}"' for c in pk_cols)
        rows = conn.execute(f'SELECT {col_list} FROM "{safe_table}"').fetchall()
        return {tuple(row) for row in rows}

    def _fetch_rows_by_pks(
        self,
        conn: sqlite3.Connection,
        table: str,
        pk_cols: list[str],
        pks: list[tuple],
    ) -> dict[tuple, tuple]:
        """
        Fetch all columns for the specified PK values.

        Chunked to stay within SQLite's bound-variable limit.
        Returns a dict mapping PK tuple → full row tuple.
        """
        safe_table = table.replace('"', '""')
        result: dict[tuple, tuple] = {}

        for i in range(0, len(pks), _CHUNK_SIZE):
            chunk = pks[i : i + _CHUNK_SIZE]

            if len(pk_cols) == 1:
                placeholders = ", ".join("?" * len(chunk))
                sql = (
                    f'SELECT * FROM "{safe_table}" '
                    f'WHERE "{pk_cols[0]}" IN ({placeholders})'
                )
                params: list = [pk[0] for pk in chunk]
            else:
                # Composite PK: (col1=? AND col2=?) OR (col1=? AND col2=?) …
                conditions = " OR ".join(
                    "(" + " AND ".join(f'"{c}"=?' for c in pk_cols) + ")"
                    for _ in chunk
                )
                sql = f'SELECT * FROM "{safe_table}" WHERE {conditions}'
                params = [val for pk in chunk for val in pk]

            for row in conn.execute(sql, params):
                row_tuple = tuple(row)
                pk_key = tuple(row[c] for c in pk_cols)
                result[pk_key] = row_tuple

        return result
