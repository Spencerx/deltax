"""Extended aggregate correctness coverage."""

import pytest

from .datasets import create_aggregate_matrix_pair
from .harness import assert_query_case
from .querygen import aggregate_extended_cases


pytestmark = pytest.mark.extended


AGGREGATE_LAYOUTS = (
    ("segment_by_group", ("group_key",), ("ts", "id")),
    ("segment_by_device", ("device_id",), ("group_key", "ts", "id")),
)

FASTPATH_MODES = (
    ("fastpath_on", "off"),
    ("fastpath_disabled", "on"),
)


@pytest.fixture(params=AGGREGATE_LAYOUTS, ids=lambda layout: layout[0])
def aggregate_matrix(db, request):
    layout_name, segment_by, order_by = request.param
    return create_aggregate_matrix_pair(
        db,
        deltax_table=f"aggregate_extended_{layout_name}",
        segment_by=segment_by,
        order_by=order_by,
    )


@pytest.mark.parametrize("case", list(aggregate_extended_cases()), ids=lambda case: case.name)
@pytest.mark.parametrize("fastpath_mode", FASTPATH_MODES, ids=lambda mode: mode[0])
def test_extended_aggregate_matrix_matches_plain_postgres(
    aggregate_matrix, db, case, fastpath_mode
):
    _, guc_value = fastpath_mode
    plain_table, deltax_table = aggregate_matrix
    db.execute(f"SET pg_deltax.disable_meta_agg_fastpath = {guc_value}")
    assert_query_case(
        db,
        case,
        plain_table=plain_table,
        deltax_table=deltax_table,
    )
