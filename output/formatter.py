"""
formatter.py — Human-readable CLI output for sqlite-diffx.

Converts a RawDiff to a plain-text summary suitable for terminal output.
No ANSI codes — output is CI-friendly and piping-safe.
"""
from __future__ import annotations

from sqlite_diffx.core.schema import RawDiff


def format_diff(diff: RawDiff) -> str:
    """
    Return a human-readable multi-line string describing all changes.

    Format::

        Schema Changes:
          [+] table new_table
          [-] table old_table
          [~] table users
              [+] column age (INTEGER)
              [-] column legacy_field (TEXT)
              [~] column email: TEXT -> VARCHAR
        Data Changes:
          users: +5 inserted, -2 deleted, ~3 updated
        Breaking Changes:
          !! DROP TABLE: old_table
          ?  TYPE CHANGE: users.email TEXT -> VARCHAR (warning)

    When there are no differences at all, returns "No differences found."
    """
    if diff.is_empty and not diff.breaking_changes:
        return "No differences found."

    parts: list[str] = []
    parts.extend(_format_schema_section(diff))
    parts.extend(_format_data_section(diff))
    parts.extend(_format_breaking_section(diff))
    return "\n".join(parts)


# ---------------------------------------------------------------------------
# Section builders
# ---------------------------------------------------------------------------


def _format_schema_section(diff: RawDiff) -> list[str]:
    lines = ["Schema Changes:"]
    sd = diff.schema
    has_entries = False

    for table in sd.added_tables:
        lines.append(f"  [+] table {table}")
        has_entries = True

    for table in sd.removed_tables:
        lines.append(f"  [-] table {table}")
        has_entries = True

    for table_name, td in sd.modified_tables.items():
        lines.append(f"  [~] table {table_name}")
        for col in td.added_columns:
            lines.append(f"      [+] column {col.name} ({col.type})")
        for col in td.removed_columns:
            lines.append(f"      [-] column {col.name} ({col.type})")
        for col_name, change in td.modified_columns.items():
            lines.append(
                f"      [~] column {col_name}: {change.old_type} -> {change.new_type}"
            )
        has_entries = True

    if not has_entries:
        lines.append("  (none)")

    return lines


def _format_data_section(diff: RawDiff) -> list[str]:
    lines = ["Data Changes:"]
    has_entries = False

    for table_name, result in diff.data.items():
        if not result.has_changes:
            continue
        has_entries = True
        suffix = " (count only — no primary key)" if result.strategy == "COUNT_ONLY" else ""
        lines.append(
            f"  {table_name}: "
            f"+{result.inserted_count} inserted, "
            f"-{result.deleted_count} deleted, "
            f"~{result.updated_count} updated"
            f"{suffix}"
        )

    if not has_entries:
        lines.append("  (none)")

    return lines


def _format_breaking_section(diff: RawDiff) -> list[str]:
    lines = ["Breaking Changes:"]

    if not diff.breaking_changes:
        lines.append("  (none)")
        return lines

    for bc in diff.breaking_changes:
        if bc.severity == "BREAKING":
            prefix = "  !!"
        else:
            prefix = "  ? "

        lines.append(f"{prefix} {bc.change_type}: {bc.description}")

    return lines
