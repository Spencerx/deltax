"""Compression codec and direct backfill correctness coverage."""

import pytest

from .datasets import create_codec_matrix_pair
from .harness import assert_query_case
from .querygen import codec_matrix_cases


pytestmark = pytest.mark.smoke


CODEC_LAYOUTS = (
    ("regular_time_ordered_s1", "regular", ("device_id",), ("ts", "id"), 1),
    ("regular_time_ordered_s2", "regular", ("device_id",), ("ts", "id"), 2),
    ("regular_time_ordered_s9", "regular", ("device_id",), ("ts", "id"), 9),
    ("regular_large_int_ordered_s17", "regular", ("device_id",), ("large_int", "ts", "id"), 17),
    ("copy_text_s1", "copy_text", (), ("ts", "id"), 1),
    ("copy_text_options_s9", "copy_text_options", (), ("ts", "id"), 9),
    ("copy_csv_s1", "copy_csv", (), ("ts", "id"), 1),
    ("copy_csv_options_s9", "copy_csv_options", (), ("ts", "id"), 9),
)


@pytest.fixture(params=CODEC_LAYOUTS, ids=lambda layout: layout[0])
def codec_matrix(db, request):
    layout_name, load_path, segment_by, order_by, segment_size = request.param
    return create_codec_matrix_pair(
        db,
        deltax_table=f"codec_matrix_{layout_name}",
        load_path=load_path,
        segment_by=segment_by,
        order_by=order_by,
        segment_size=segment_size,
    )


@pytest.mark.parametrize("case", list(codec_matrix_cases()), ids=lambda case: case.name)
def test_codec_matrix_matches_plain_postgres(codec_matrix, db, case):
    plain_table, deltax_table = codec_matrix
    assert_query_case(
        db,
        case,
        plain_table=plain_table,
        deltax_table=deltax_table,
    )


@pytest.mark.parametrize(
    "params",
    [
        ("dict_text", "alpha", "small_int", "-5", "large_int", "100000000000"),
        ("dict_text", "beta", "small_int", "-32768", "large_int", "9223372036854775806"),
        ("dict_text", None, "small_int", "0", "large_int", "0"),
    ],
    ids=("common_values", "boundary_values", "null_dictionary"),
)
def test_codec_matrix_prepared_predicates_match_plain_postgres(codec_matrix, db, params):
    plain_table, deltax_table = codec_matrix
    _, dict_text, _, small_int, _, large_int = params

    db.execute(
        f"""
        PREPARE plain_codec_predicates(text, integer, bigint) AS
        SELECT id, dict_text, small_int, int_val, large_int, unique_text
        FROM {plain_table}
        WHERE dict_text IS NOT DISTINCT FROM $1
           OR small_int = $2
           OR large_int >= $3
        ORDER BY id
        """
    )
    db.execute(
        f"""
        PREPARE deltax.deltax_codec_predicates(text, integer, bigint) AS
        SELECT id, dict_text, small_int, int_val, large_int, unique_text
        FROM {deltax_table}
        WHERE dict_text IS NOT DISTINCT FROM $1
           OR small_int = $2
           OR large_int >= $3
        ORDER BY id
        """
    )

    dict_literal = "NULL" if dict_text is None else f"'{dict_text}'"
    plain_rows = db.execute(
        f"EXECUTE plain_codec_predicates({dict_literal}, {small_int}, {large_int})"
    ).fetchall()
    deltax_rows = db.execute(
        f"EXECUTE deltax.deltax_codec_predicates({dict_literal}, {small_int}, {large_int})"
    ).fetchall()
    assert plain_rows == deltax_rows
