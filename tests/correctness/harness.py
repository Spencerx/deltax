"""Execution harness for PostgreSQL-vs-deltax correctness tests."""

from __future__ import annotations

from dataclasses import dataclass

from .comparators import CompareResult, compare


@dataclass(frozen=True)
class QueryCase:
    name: str
    sql: str
    comparator: str = "ordered_exact"


@dataclass(frozen=True)
class CaseResult:
    case: QueryCase
    plain_rows: list
    deltax_rows: list
    comparison: CompareResult


def run_query_case(conn, case: QueryCase, *, plain_table: str, deltax_table: str) -> CaseResult:
    """Run one query case against the plain PostgreSQL and deltax table.

    Query SQL should use ``{table}`` as the table placeholder. Keeping the
    placeholder small makes generated and hand-written cases easy to read.
    """
    plain_sql = case.sql.format(table=plain_table)
    deltax_sql = case.sql.format(table=deltax_table)

    plain_rows = conn.execute(plain_sql).fetchall()
    deltax_rows = conn.execute(deltax_sql).fetchall()
    comparison = compare(case.comparator, plain_rows, deltax_rows)
    return CaseResult(case, plain_rows, deltax_rows, comparison)


def assert_query_case(conn, case: QueryCase, *, plain_table: str, deltax_table: str) -> None:
    result = run_query_case(
        conn,
        case,
        plain_table=plain_table,
        deltax_table=deltax_table,
    )
    if result.comparison.ok:
        return

    raise AssertionError(
        f"correctness case {case.name!r} failed with comparator "
        f"{case.comparator!r}: {result.comparison.detail}\n"
        f"SQL: {case.sql}\n"
        f"plain sample: {result.plain_rows[:5]!r}\n"
        f"deltax sample: {result.deltax_rows[:5]!r}"
    )
