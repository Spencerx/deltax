"""Result comparators for pg_deltax correctness tests.

The harness treats plain PostgreSQL as the reference, but not every SQL query
has one byte-identical row ordering. Comparators make those semantics explicit
per case so relaxed comparisons are deliberate and reviewable.
"""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from decimal import Decimal
from typing import Iterable, Sequence


Row = Sequence[object]
Rows = Sequence[Row]


@dataclass(frozen=True)
class CompareResult:
    ok: bool
    detail: str


def _cell_key(value: object) -> object:
    if value is None:
        return None
    if isinstance(value, Decimal):
        return str(value)
    return value


def _row_key(row: Row) -> tuple[object, ...]:
    return tuple(_cell_key(value) for value in row)


def ordered_exact(plain: Rows, deltax: Rows) -> CompareResult:
    if len(plain) != len(deltax):
        return CompareResult(False, f"row count: plain={len(plain)} deltax={len(deltax)}")

    for idx, (p_row, d_row) in enumerate(zip(plain, deltax)):
        if _row_key(p_row) != _row_key(d_row):
            return CompareResult(
                False,
                f"first mismatch at row {idx}: plain={p_row!r} deltax={d_row!r}",
            )
    return CompareResult(True, f"{len(plain)} rows")


def unordered_exact(plain: Rows, deltax: Rows) -> CompareResult:
    plain_bag = Counter(_row_key(row) for row in plain)
    deltax_bag = Counter(_row_key(row) for row in deltax)
    if plain_bag == deltax_bag:
        return CompareResult(True, f"{len(plain)} rows")

    missing = list((plain_bag - deltax_bag).elements())[:3]
    extra = list((deltax_bag - plain_bag).elements())[:3]
    return CompareResult(
        False,
        f"row bag mismatch: missing_from_deltax={missing!r} extra_in_deltax={extra!r}",
    )


def limit_ties(plain: Rows, deltax: Rows) -> CompareResult:
    """Relax comparison for non-unique ORDER BY ... LIMIT queries.

    This policy is intentionally weak: it only checks row count and result-set
    overlap. Test authors should prefer adding a deterministic tiebreaker and
    using ordered_exact whenever possible.
    """
    if len(plain) != len(deltax):
        return CompareResult(False, f"row count: plain={len(plain)} deltax={len(deltax)}")

    plain_set = {_row_key(row) for row in plain}
    deltax_set = {_row_key(row) for row in deltax}
    overlap = len(plain_set & deltax_set)
    if overlap == min(len(plain_set), len(deltax_set)):
        return CompareResult(True, f"{len(plain)} rows, tie-relaxed")
    return CompareResult(
        False,
        f"tie-relaxed mismatch: overlap={overlap} plain_distinct={len(plain_set)} "
        f"deltax_distinct={len(deltax_set)}",
    )


def float_tolerant(
    plain: Rows,
    deltax: Rows,
    *,
    abs_tol: float = 1e-8,
    rel_tol: float = 1e-9,
) -> CompareResult:
    if len(plain) != len(deltax):
        return CompareResult(False, f"row count: plain={len(plain)} deltax={len(deltax)}")

    for ridx, (p_row, d_row) in enumerate(zip(plain, deltax)):
        if len(p_row) != len(d_row):
            return CompareResult(False, f"column count mismatch at row {ridx}")
        for cidx, (p_cell, d_cell) in enumerate(zip(p_row, d_row)):
            if p_cell is None or d_cell is None:
                if p_cell is not None or d_cell is not None:
                    return CompareResult(False, f"NULL mismatch at row {ridx}, col {cidx}")
                continue
            if isinstance(p_cell, (int, float, Decimal)) and isinstance(
                d_cell, (int, float, Decimal)
            ):
                p_float = float(p_cell)
                d_float = float(d_cell)
                if p_float == d_float:
                    continue
                if abs(p_float - d_float) <= max(
                    abs_tol, rel_tol * max(abs(p_float), abs(d_float))
                ):
                    continue
                return CompareResult(
                    False,
                    f"numeric mismatch at row {ridx}, col {cidx}: "
                    f"plain={p_cell!r} deltax={d_cell!r}",
                )
            if p_cell != d_cell:
                return CompareResult(
                    False,
                    f"cell mismatch at row {ridx}, col {cidx}: "
                    f"plain={p_cell!r} deltax={d_cell!r}",
                )
    return CompareResult(True, f"{len(plain)} rows")


COMPARATORS = {
    "ordered_exact": ordered_exact,
    "unordered_exact": unordered_exact,
    "limit_ties": limit_ties,
    "float_tolerant": float_tolerant,
}


def compare(policy: str, plain: Rows, deltax: Rows) -> CompareResult:
    try:
        comparator = COMPARATORS[policy]
    except KeyError as exc:
        known = ", ".join(sorted(COMPARATORS))
        raise ValueError(f"unknown comparison policy {policy!r}; expected one of: {known}") from exc
    return comparator(plain, deltax)
