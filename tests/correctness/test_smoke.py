"""Smoke coverage for the correctness harness itself."""

import pytest

from .datasets import create_tiny_events_pair
from .harness import assert_query_case
from .querygen import curated_smoke_cases


pytestmark = pytest.mark.smoke


@pytest.fixture()
def tiny_events(db):
    return create_tiny_events_pair(db)


@pytest.mark.parametrize("case", list(curated_smoke_cases()), ids=lambda case: case.name)
def test_tiny_events_matches_plain_postgres(tiny_events, db, case):
    plain_table, deltax_table = tiny_events
    assert_query_case(
        db,
        case,
        plain_table=plain_table,
        deltax_table=deltax_table,
    )
