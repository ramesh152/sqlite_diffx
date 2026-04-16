"""
main.py — CLI entry point for sqlite-diffx.

Registered as the 'diffx' console script via pyproject.toml.

Commands
--------
diffx diff db1.sqlite db2.sqlite
    Show human-readable schema + data diff.

diffx diff db1.sqlite db2.sqlite --json
    Output diff as JSON.

diffx diff db1.sqlite db2.sqlite --fail-on-breaking
    Exit with code 1 if any BREAKING-severity changes are found.

diffx diff db1.sqlite db2.sqlite --no-data
    Schema diff only (skip row-level comparison).

diffx patch db1.sqlite db2.sqlite
    Print a forward-only SQL migration patch.

Exit codes
----------
0  Success (no breaking changes, or --fail-on-breaking not set)
1  Breaking changes found (only when --fail-on-breaking is passed)
2  Error (file not found, not a valid SQLite database, etc.)
"""
from __future__ import annotations

import argparse
import sys

from sqlite_diffx.api.public import compare


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="diffx",
        description="Git-like diff tool for SQLite databases.",
    )

    sub = parser.add_subparsers(dest="command", required=True)

    # ------------------------------------------------------------------ diff
    diff_p = sub.add_parser(
        "diff",
        help="Show differences between two SQLite databases.",
    )
    diff_p.add_argument("db1", help="Source (old) database path")
    diff_p.add_argument("db2", help="Target (new) database path")
    diff_p.add_argument(
        "--json",
        action="store_true",
        dest="as_json",
        help="Output differences as JSON.",
    )
    diff_p.add_argument(
        "--fail-on-breaking",
        action="store_true",
        help="Exit with code 1 if any breaking changes are detected.",
    )
    diff_p.add_argument(
        "--no-data",
        action="store_true",
        help="Skip row-level data diff (schema changes only).",
    )
    diff_p.set_defaults(func=cmd_diff)

    # ----------------------------------------------------------------- patch
    patch_p = sub.add_parser(
        "patch",
        help="Print a forward-only SQL migration patch.",
    )
    patch_p.add_argument("db1", help="Source (old) database path")
    patch_p.add_argument("db2", help="Target (new) database path")
    patch_p.set_defaults(func=cmd_patch)

    return parser


def cmd_diff(args: argparse.Namespace) -> int:
    """Handle the 'diffx diff' subcommand. Returns exit code."""
    try:
        result = compare(args.db1, args.db2, include_data=not args.no_data)
    except FileNotFoundError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 2
    except Exception as exc:  # noqa: BLE001
        print(f"Error reading database: {exc}", file=sys.stderr)
        return 2

    if args.as_json:
        print(result.to_json())
    else:
        print(result.summary())

    if args.fail_on_breaking and result.has_breaking_changes():
        return 1
    return 0


def cmd_patch(args: argparse.Namespace) -> int:
    """Handle the 'diffx patch' subcommand. Returns exit code."""
    try:
        result = compare(args.db1, args.db2, include_data=False)
    except FileNotFoundError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 2
    except Exception as exc:  # noqa: BLE001
        print(f"Error reading database: {exc}", file=sys.stderr)
        return 2

    print(result.to_sql())
    return 0


def main() -> None:
    """Entry point for the 'diffx' CLI command."""
    parser = build_parser()
    args = parser.parse_args()
    sys.exit(args.func(args))


if __name__ == "__main__":
    main()
