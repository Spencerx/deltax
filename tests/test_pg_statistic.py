"""Integration tests for pg_statistic / pg_class.reltuples population on
compressed partitions.

The purpose is to fix PG's planner falling back to default selectivity
(0.005 numeric-eq, ~2.5e-5 text-eq) on compressed partitions because
they have `pg_class.reltuples = 0` and no `pg_statistic` rows.
"""

from datetime import datetime, timedelta, timezone


def _seed(db, n_partitions=4, rows_per_partition=50_000, high_card=5000):
    """Create a partitioned deltax table with columns of controlled
    cardinality / null fraction and compress every populated partition.

    - `uid`  — INT8, `high_card` distinct values (join key) → histogram slot.
    - `kind` — TEXT, 5 distinct values (low-cardinality enum) → MCV slot.
    - `skew` — TEXT, 4 distinct values with a deliberately *skewed* frequency
               (hot 50% / warm 30% / cool 15% / rare 5%) → MCV slot. Ground
               truth for `most_common_freqs`: a regression to uniform `1/n`
               would estimate every value at 25%, which these tests catch.
    - `val`  — FLOAT8, ~unique (measurement column) → stadistinct only.
    - `note` — TEXT, NULL in exactly 25% of rows (one non-null value) →
               ground truth for `stanullfrac`. A bug that hardcodes nullfrac
               to 0 (Q06) or 1.0 (the `_nonnull_count == 0` trap) shows up
               here as a wrong `null_frac` regardless of which query runs.
    """
    db.execute(
        "CREATE TABLE events ("
        "  ts TIMESTAMPTZ NOT NULL,"
        "  uid BIGINT,"
        "  kind TEXT,"
        "  skew TEXT,"
        "  val FLOAT8,"
        "  note TEXT"
        ")"
    )
    db.execute(
        "SELECT deltax.deltax_create_table('events', 'ts', '1 day', %s)",
        (n_partitions - 1,),
    )
    db.execute(
        "SELECT deltax.deltax_enable_compression('events', order_by => ARRAY['ts'])"
    )
    db.commit()

    today = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    window_us = 22 * 3600 * 1_000_000
    spacing_us = max(1, window_us // max(1, rows_per_partition - 1))

    for p in range(n_partitions):
        part_start = today - timedelta(days=1) + timedelta(days=p) + timedelta(hours=1)
        db.execute(
            "INSERT INTO events (ts, uid, kind, skew, val, note) "
            "SELECT %s::timestamptz + (i::bigint * %s::bigint * interval '1 microsecond'), "
            "       (i %% %s)::bigint, "
            "       CASE WHEN i %% 5 = 0 THEN 'A' "
            "            WHEN i %% 5 = 1 THEN 'B' "
            "            WHEN i %% 5 = 2 THEN 'C' "
            "            WHEN i %% 5 = 3 THEN 'D' "
            "            ELSE 'E' END, "
            # Skewed: i%20 in 0..9 → 'hot' (50%), 10..15 → 'warm' (30%),
            # 16..18 → 'cool' (15%), 19 → 'rare' (5%). Exact when
            # rows_per_partition is a multiple of 20.
            "       CASE WHEN i %% 20 < 10 THEN 'hot' "
            "            WHEN i %% 20 < 16 THEN 'warm' "
            "            WHEN i %% 20 < 19 THEN 'cool' "
            "            ELSE 'rare' END, "
            "       random(), "
            "       CASE WHEN i %% 4 = 0 THEN NULL ELSE 'x' END "
            "FROM generate_series(0, %s) i",
            (part_start, spacing_us, high_card, rows_per_partition - 1),
        )
    db.commit()

    assert db.execute("SELECT count(*) FROM events_default").fetchone()[0] == 0

    for (name,) in db.execute(
        "SELECT partition_name FROM deltax.deltax_partition_info('events') "
        "ORDER BY range_start"
    ).fetchall():
        cnt = db.execute(f'SELECT count(*) FROM "{name}"').fetchone()[0]
        if cnt > 0:
            db.execute("SELECT deltax.deltax_compress_partition(%s)", (name,))
    db.commit()


def _first_compressed_partition(db):
    row = db.execute(
        "SELECT table_name FROM deltax.deltax_partition WHERE is_compressed = true "
        "ORDER BY range_start LIMIT 1"
    ).fetchone()
    assert row is not None, "no compressed partitions"
    return row[0]


def _stats_for(db, part_name, attname):
    """Return (stadistinct, stanullfrac, stawidth) from pg_statistic."""
    row = db.execute(
        "SELECT s.stadistinct, s.stanullfrac, s.stawidth "
        "FROM pg_statistic s "
        "JOIN pg_attribute a ON a.attrelid = s.starelid AND a.attnum = s.staattnum "
        "WHERE s.starelid = %s::regclass AND a.attname = %s",
        (part_name, attname),
    ).fetchone()
    return row  # None if no stats


def _parse_pg_array(text):
    """Parse a PostgreSQL array text literal (`{a,b,"c,d"}`) into a list of
    element strings. Handles double-quoted elements (timestamps, values with
    commas); good enough for the simple types this test exercises."""
    if text is None:
        return []
    s = text.strip()
    assert s.startswith("{") and s.endswith("}"), f"not an array literal: {text!r}"
    s = s[1:-1]
    if s == "":
        return []
    out, buf, in_quotes, i = [], [], False, 0
    while i < len(s):
        c = s[i]
        if in_quotes:
            if c == "\\" and i + 1 < len(s):
                buf.append(s[i + 1])
                i += 2
                continue
            if c == '"':
                in_quotes = False
            else:
                buf.append(c)
        elif c == '"':
            in_quotes = True
        elif c == ",":
            out.append("".join(buf))
            buf = []
        else:
            buf.append(c)
        i += 1
    out.append("".join(buf))
    return out


def _pg_stats(db, rel, attname, inherited=False):
    """Return a dict of decoded slot values from the `pg_stats` view for one
    column. `pg_stats` decodes the anyarray slots (`most_common_vals`,
    `histogram_bounds`) and exposes `inherited` (stainherit) so we can check
    both the per-partition (`inherited=False`) and the merged parent
    (`inherited=True`) rows. None if the row is absent."""
    schema, _, table = rel.rpartition(".")
    table = table or rel
    schema = schema or "public"
    row = db.execute(
        "SELECT null_frac, n_distinct, "
        "       most_common_vals::text, most_common_freqs, "
        "       histogram_bounds::text "
        "FROM pg_stats "
        "WHERE schemaname = %s AND tablename = %s AND attname = %s "
        "  AND inherited = %s",
        (schema, table, attname, inherited),
    ).fetchone()
    if row is None:
        return None
    return {
        "null_frac": row[0],
        "n_distinct": row[1],
        "most_common_vals": _parse_pg_array(row[2]),
        "most_common_freqs": list(row[3]) if row[3] is not None else None,
        "histogram_bounds": _parse_pg_array(row[4]),
    }


def _slot1_ops(db, rel, attname, inherited=False):
    """Return (stakind1, staop1, stacoll1) read straight from `pg_statistic`.
    `pg_stats` hides `staop`/`stacoll`, but the NULL-`stacoll1` bug (PG then
    silently ignores the slot) is invisible in the decoded view — it can only
    be caught by reading the raw catalog. `staop`/`stacoll` are NOT NULL
    columns whose unused value is 0; a *NULL* (None in Python) is the bug."""
    row = db.execute(
        "SELECT s.stakind1, s.staop1::int8, s.stacoll1::int8 "
        "FROM pg_statistic s "
        "JOIN pg_attribute a ON a.attrelid = s.starelid AND a.attnum = s.staattnum "
        "WHERE s.starelid = %s::regclass AND a.attname = %s "
        "  AND s.stainherit = %s",
        (rel, attname, inherited),
    ).fetchone()
    return row  # (stakind1, staop1, stacoll1) or None


def test_compress_populates_pg_statistic(db):
    """After compression, pg_statistic must have a row per non-dropped
    column with stadistinct reflecting the partition-level HLL merge."""
    _seed(db, n_partitions=2, rows_per_partition=50_000, high_card=5000)

    part = _first_compressed_partition(db)
    # 50K rows per partition, 5000 distinct uid values → stadistinct should be
    # a positive absolute around 5000 (less than 10% of 50K).
    stats = _stats_for(db, part, "uid")
    assert stats is not None, f"no pg_statistic row for {part}.uid"
    stadist, nullfrac, width = stats
    # HLL tolerance ~2%, random-mod uid distribution gives exact 5000 very
    # reliably for this fixture.
    assert 4800 <= stadist <= 5200, f"stadistinct(uid)={stadist}, expected ~5000"
    assert 0.0 <= nullfrac < 0.01, f"nullfrac(uid)={nullfrac}"
    assert width == 8, f"stawidth(uid)={width} (BIGINT = 8)"

    # kind has 5 distinct values → stadistinct should be ≈5.
    stats = _stats_for(db, part, "kind")
    assert stats is not None
    stadist, _nullfrac, _width = stats
    assert 4 <= stadist <= 6, f"stadistinct(kind)={stadist}, expected ~5"


def test_compress_updates_reltuples(db):
    """pg_class.reltuples must reflect actual row count so PG's selectivity
    estimators stop using default 0.005 for eq."""
    _seed(db, n_partitions=2, rows_per_partition=50_000, high_card=5000)
    part = _first_compressed_partition(db)
    rel_tuples = db.execute(
        "SELECT reltuples::bigint FROM pg_class WHERE oid = %s::regclass",
        (part,),
    ).fetchone()[0]
    # We expect exactly 50K rows per partition from the fixture.
    assert 49_000 <= rel_tuples <= 50_000, f"reltuples={rel_tuples}, expected ~50000"


def test_plan_row_estimate_uses_populated_stats(db):
    """EXPLAIN row-estimate after compression should match
    rel_tuples / ndistinct (not default 0.5%)."""
    _seed(db, n_partitions=2, rows_per_partition=50_000, high_card=5000)

    # Equality on the high-card column: 50K rows / 5000 distinct ≈ 10.
    plan = db.execute(
        "EXPLAIN (FORMAT JSON) SELECT * FROM events WHERE uid = 42"
    ).fetchone()[0]
    import json
    root = json.loads(plan) if isinstance(plan, str) else plan
    # Walk the plan tree for the lowest `Plan Rows` (usually the scan).
    def find_scan_rows(node):
        rows = [node.get("Plan Rows", 0)]
        for child in node.get("Plans", []) or []:
            rows += find_scan_rows(child)
        return rows
    rows = find_scan_rows(root[0]["Plan"])
    assert rows, "no plan rows"
    # With 50K * 2 partitions = 100K total and ndistinct=5000, expect ~20.
    # Default selectivity (0.005) would give 50K * 0.005 * 2 = 500. So
    # anything under 100 signals we're on the populated-stats path.
    total_rows = max(rows)
    assert total_rows < 100, (
        f"Plan Rows={total_rows} — equality selectivity looks like the default "
        f"0.005 rather than 1/ndistinct. pg_statistic may not be populated."
    )


def test_analyze_is_intercepted_on_compressed_partition(db):
    """Running `ANALYZE <compressed_partition>` must not clobber the
    pg_statistic rows we maintain at compress time."""
    _seed(db, n_partitions=2, rows_per_partition=50_000, high_card=5000)
    part = _first_compressed_partition(db)

    before = _stats_for(db, part, "uid")
    assert before is not None

    db.rollback()
    db.autocommit = True
    db.execute(f'ANALYZE "{part}"')
    db.autocommit = False

    after = _stats_for(db, part, "uid")
    assert after is not None, (
        "pg_statistic row was deleted by ANALYZE — the ProcessUtility "
        "hook should have filtered this compressed partition out"
    )
    # Values should be identical — ANALYZE never touched them.
    assert before[0] == after[0], f"stadistinct changed: {before[0]} -> {after[0]}"


def test_deltax_analyze_partition_is_idempotent(db):
    """Calling deltax_analyze_partition on a freshly-compressed partition
    should produce the same stats (within HLL tolerance)."""
    _seed(db, n_partitions=2, rows_per_partition=50_000, high_card=5000)
    part = _first_compressed_partition(db)

    before = _stats_for(db, part, "uid")
    db.execute("SELECT deltax.deltax_analyze_partition(%s)", (part,))
    db.commit()
    after = _stats_for(db, part, "uid")

    assert before is not None and after is not None
    row_count = db.execute(
        "SELECT reltuples::bigint FROM pg_class WHERE oid = %s::regclass",
        (part,),
    ).fetchone()[0]
    # Translate PG's signed stadistinct into absolute distinct count
    # (positive = count, negative fraction = -frac * rowcount) so HLL
    # and SUM-capped fallback estimates are on the same scale.
    def absolute(nd, rc):
        return nd if nd >= 0 else -nd * rc
    before_abs = absolute(before[0], row_count)
    after_abs = absolute(after[0], row_count)
    # The fallback SUM-capped path is less accurate than the HLL merge
    # (especially for time-clustered keys); allow a 3× tolerance.
    assert max(before_abs, after_abs) / max(min(before_abs, after_abs), 1) < 3.0, (
        f"stadistinct drift too large: before={before[0]} ({before_abs}), "
        f"after={after[0]} ({after_abs})"
    )


def test_autovacuum_disabled_on_compressed(db):
    """After compression, the partition should have
    autovacuum_enabled = off so autovacuum doesn't clobber stats."""
    _seed(db, n_partitions=2, rows_per_partition=50_000, high_card=5000)
    part = _first_compressed_partition(db)
    options = db.execute(
        "SELECT reloptions FROM pg_class WHERE oid = %s::regclass",
        (part,),
    ).fetchone()[0]
    opts = options or []
    assert any("autovacuum_enabled=off" in o for o in opts), (
        f"autovacuum_enabled=off not in reloptions: {opts}"
    )


# ---------------------------------------------------------------------------
# Slot-content validation (the layer that catches "stat written but wrong").
# pg_stats decodes the slot arrays; pg_statistic carries the raw staop/stacoll.
# ---------------------------------------------------------------------------

def test_mcv_slot_matches_distinct_set(db):
    """The MCV slot for a low-cardinality text column must list exactly the
    column's distinct values (so equality on an absent value estimates ~0 —
    Q19), carry per-value frequencies that sum to ~1.0, and have non-null
    staop/stacoll (a NULL stacoll makes PG silently ignore the slot)."""
    _seed(db, n_partitions=2, rows_per_partition=50_000, high_card=5000)
    part = _first_compressed_partition(db)

    s = _pg_stats(db, part, "kind", inherited=False)
    assert s is not None, "no pg_stats row for kind"
    assert set(s["most_common_vals"]) == {"A", "B", "C", "D", "E"}, (
        f"MCV stavalues {s['most_common_vals']} != true distinct set"
    )
    freqs = s["most_common_freqs"]
    assert freqs is not None and len(freqs) == 5, f"most_common_freqs={freqs}"
    assert all(f > 0 for f in freqs), f"non-positive MCV freq: {freqs}"
    assert abs(sum(freqs) - 1.0) < 0.01, f"MCV freqs sum {sum(freqs)} != ~1.0"

    stakind1, staop1, stacoll1 = _slot1_ops(db, part, "kind", inherited=False)
    assert stakind1 == 1, f"slot 1 stakind={stakind1}, expected 1 (MCV)"
    assert staop1 not in (None, 0), f"MCV staop1={staop1} (NULL/0 → slot ignored)"
    assert stacoll1 not in (None, 0), (
        f"MCV stacoll1={stacoll1} — text MCV needs a real collation"
    )


def test_histogram_slot_brackets_min_max(db):
    """The histogram slot for an ordered int column must bracket the true
    [min, max], be strictly ascending, and have a non-null staop (btree `<`).
    stacoll is legitimately 0 for a non-collatable type — but it must not be
    NULL, which would make PG drop the slot (the bug that neutralised the
    order-by histogram)."""
    _seed(db, n_partitions=2, rows_per_partition=50_000, high_card=5000)
    part = _first_compressed_partition(db)

    s = _pg_stats(db, part, "uid", inherited=False)
    assert s is not None, "no pg_stats row for uid"
    bounds = [int(b) for b in s["histogram_bounds"]]
    assert len(bounds) >= 2, f"histogram_bounds too short: {bounds}"
    assert bounds == sorted(bounds) and len(set(bounds)) == len(bounds), (
        f"histogram_bounds not strictly ascending: {bounds}"
    )
    # uid = i % 5000 over 50K rows → every partition spans 0..4999.
    assert bounds[0] <= 0 and bounds[-1] >= 4999, (
        f"histogram_bounds {bounds} don't bracket true [0, 4999]"
    )

    stakind1, staop1, stacoll1 = _slot1_ops(db, part, "uid", inherited=False)
    assert stakind1 == 2, f"slot 1 stakind={stakind1}, expected 2 (histogram)"
    assert staop1 not in (None, 0), f"histogram staop1={staop1} (NULL/0 → slot ignored)"
    assert stacoll1 is not None, (
        "histogram stacoll1 is NULL — PG ignores a slot with NULL stacoll "
        "(must be a non-null 0 for a non-collatable type)"
    )


def test_controlled_nullfrac_matches_ground_truth(db):
    """`note` is NULL in exactly 25% of rows. stanullfrac must reflect that
    on both the child partition and the merged parent — catching both the
    hardcoded-0 (Q06) and the all-null 1.0 (`_nonnull_count == 0`) traps."""
    _seed(db, n_partitions=4, rows_per_partition=50_000, high_card=5000)

    # True null fraction straight from the data.
    true_nf = db.execute(
        "SELECT avg((note IS NULL)::int)::float8 FROM events"
    ).fetchone()[0]
    assert abs(true_nf - 0.25) < 0.01, f"fixture null frac drifted: {true_nf}"

    part = _first_compressed_partition(db)
    child = _pg_stats(db, part, "note", inherited=False)
    assert child is not None, "no pg_stats row for note"
    assert abs(child["null_frac"] - 0.25) < 0.03, (
        f"child null_frac(note)={child['null_frac']}, expected ~0.25"
    )

    # Parent (merged) stats only exist after a table-level analyze.
    db.execute("SELECT deltax.deltax_analyze_table('events')")
    db.commit()
    parent = _pg_stats(db, "events", "note", inherited=True)
    assert parent is not None, "no inherited pg_stats row for note"
    assert abs(parent["null_frac"] - 0.25) < 0.03, (
        f"parent null_frac(note)={parent['null_frac']}, expected ~0.25"
    )


def test_absent_value_equality_estimates_near_zero(db):
    """Equality on a value the column never contains must estimate ~0, not
    rows/ndistinct. This is the Q19 disaster (871 s): 'Shipped' on an
    event_type that never holds it estimated 1/ndistinct → 20M-row NestLoop."""
    import json

    _seed(db, n_partitions=2, rows_per_partition=50_000, high_card=5000)

    plan = db.execute(
        "EXPLAIN (FORMAT JSON) SELECT * FROM events WHERE kind = 'Z'"
    ).fetchone()[0]
    root = json.loads(plan) if isinstance(plan, str) else plan

    def find_scan_rows(node):
        rows = [node.get("Plan Rows", 0)]
        for child in node.get("Plans", []) or []:
            rows += find_scan_rows(child)
        return rows

    est = max(find_scan_rows(root[0]["Plan"]))
    # 'Z' is absent → MCV gives ~0. Without the MCV, 1/ndistinct (=1/5) over
    # 100K rows estimates ~20K. Anything in the low tens means the absent-value
    # path is working (PG clamps a 0-selectivity scan up to 1 row per child).
    assert est < 50, (
        f"Plan Rows={est} for an absent value — equality looks like "
        f"1/ndistinct rather than the MCV's ~0 (Q19 regression)"
    )


def test_parent_histogram_is_multibucket_and_ascending(db):
    """The merged parent histogram on the order-by column must be multi-bucket
    (per-partition mins + global max) and strictly ascending — the structure
    that fixes range selectivity collapsing to rows=8 (Q30)."""
    _seed(db, n_partitions=4, rows_per_partition=40_000, high_card=5000)

    db.execute("SELECT deltax.deltax_analyze_table('events')")
    db.commit()

    s = _pg_stats(db, "events", "ts", inherited=True)
    assert s is not None, "no inherited pg_stats row for ts"
    bounds = s["histogram_bounds"]
    # 4 disjoint daily partitions → 4 mins + 1 global max ≈ 5 bounds. Allow
    # dedup but require more than the trivial 2-point [min, max].
    assert len(bounds) >= 3, (
        f"parent ts histogram only {len(bounds)} bounds — partition mins "
        f"didn't merge into a multi-bucket histogram: {bounds}"
    )
    # ISO-8601 timestamp text sorts chronologically; require strictly ascending.
    assert bounds == sorted(bounds) and len(set(bounds)) == len(bounds), (
        f"parent ts histogram not strictly ascending: {bounds}"
    )

    # Merged n_distinct on the join key should reflect the table-wide HLL,
    # not a per-partition value. 5000 distinct uids, > 10% of 160K? No —
    # 5000/160K = 3%, so PG keeps the absolute (positive) form.
    su = _pg_stats(db, "events", "uid", inherited=True)
    assert su is not None and 4500 <= su["n_distinct"] <= 5500, (
        f"parent n_distinct(uid)={su['n_distinct'] if su else None}, expected ~5000"
    )


# ---------------------------------------------------------------------------
# Real MCV frequencies (P1). The `skew` column is hot 50% / warm 30% /
# cool 15% / rare 5%; a regression to the old uniform 1/n would put every
# value at 25%, which both layers below catch.
# ---------------------------------------------------------------------------

TRUE_SKEW = {"hot": 0.50, "warm": 0.30, "cool": 0.15, "rare": 0.05}


def test_mcv_freqs_match_skewed_distribution(db):
    """L2: most_common_freqs must reflect the real per-value frequency, not a
    uniform 1/n — on both the child partition and the merged parent."""
    _seed(db, n_partitions=2, rows_per_partition=50_000, high_card=5000)

    def assert_skew_freqs(s, where):
        assert s is not None, f"no pg_stats row for skew ({where})"
        vals, freqs = s["most_common_vals"], s["most_common_freqs"]
        assert freqs is not None, f"no most_common_freqs ({where})"
        assert set(vals) == set(TRUE_SKEW), f"MCV vals {vals} != {set(TRUE_SKEW)} ({where})"
        by_val = dict(zip(vals, freqs))
        for v, want in TRUE_SKEW.items():
            assert abs(by_val[v] - want) < 0.02, (
                f"freq({v})={by_val[v]:.3f}, expected ~{want} ({where}); "
                f"uniform 1/n would be 0.25"
            )

    part = _first_compressed_partition(db)
    assert_skew_freqs(_pg_stats(db, part, "skew", inherited=False), "child")

    db.execute("SELECT deltax.deltax_analyze_table('events')")
    db.commit()
    assert_skew_freqs(_pg_stats(db, "events", "skew", inherited=True), "parent")


def test_skewed_value_estimates_track_real_frequency(db):
    """L3: the planner's row estimate for an equality on the skewed column must
    track the value's real frequency — hot ~50% of rows, rare ~5%, absent ~0.
    Uniform 1/n would estimate every present value at ~25%."""
    import json

    _seed(db, n_partitions=2, rows_per_partition=50_000, high_card=5000)
    db.execute("SELECT deltax.deltax_analyze_table('events')")
    db.commit()
    total = db.execute("SELECT count(*) FROM events").fetchone()[0]

    def scan_est(value):
        plan = db.execute(
            "EXPLAIN (FORMAT JSON) SELECT * FROM events WHERE skew = %s", (value,)
        ).fetchone()[0]
        root = json.loads(plan) if isinstance(plan, str) else plan

        def rows(node):
            out = [node.get("Plan Rows", 0)]
            for child in node.get("Plans", []) or []:
                out += rows(child)
            return out

        return max(rows(root[0]["Plan"]))

    hot, rare, absent = scan_est("hot"), scan_est("rare"), scan_est("nope")

    assert 0.40 * total <= hot <= 0.60 * total, (
        f"hot estimate {hot} not ~50% of {total} (uniform 1/n → ~25%)"
    )
    assert 0.02 * total <= rare <= 0.10 * total, (
        f"rare estimate {rare} not ~5% of {total} (uniform 1/n → ~25%)"
    )
    assert absent < 50, f"absent-value estimate {absent} should be ~0"
    assert hot > rare > absent, (
        f"estimates not monotone with frequency: hot={hot} rare={rare} absent={absent}"
    )


def test_in_range_estimate_is_substantial(db):
    """L3: a range over the order-by column covering ~half the table's time
    span must estimate a substantial fraction of rows — not collapse to a
    handful. This is the Q30 trap (a range estimated rows=8 vs 4.2M) that a
    missing or neutralised parent histogram caused."""
    import json

    _seed(db, n_partitions=4, rows_per_partition=40_000, high_card=5000)
    db.execute("SELECT deltax.deltax_analyze_table('events')")
    db.commit()

    total = db.execute("SELECT count(*) FROM events").fetchone()[0]
    _lo, _hi, mid = db.execute(
        "SELECT min(ts), max(ts), min(ts) + (max(ts) - min(ts)) / 2 FROM events"
    ).fetchone()

    plan = db.execute(
        "EXPLAIN (FORMAT JSON) SELECT * FROM events WHERE ts <= %s", (mid,)
    ).fetchone()[0]
    root = json.loads(plan) if isinstance(plan, str) else plan

    def scan_rows(node):
        out = [node.get("Plan Rows", 0)]
        for child in node.get("Plans", []) or []:
            out += scan_rows(child)
        return out

    est = max(scan_rows(root[0]["Plan"]))
    # ~half the time span → ~half the rows. A rows=8 collapse (or default tiny
    # selectivity) would fall far below this; a generous quarter-to-nearly-all
    # band proves the histogram is doing real range selectivity.
    assert 0.25 * total <= est <= 0.90 * total, (
        f"range ts<=midpoint estimate {est} not ~half of {total} — parent "
        f"histogram range selectivity may have collapsed (the Q30 rows=8 trap)"
    )


def test_groupby_estimate_clamped_to_filtered_input(db):
    """A high-cardinality GROUP BY under a selective WHERE must estimate output
    groups bounded by the post-filter input rows, not the grouping column's full
    n_distinct. Guards the DeltaXAgg group-count clamp (hook.rs): before it,
    `GROUP BY uid` (5000 distinct) after a filter leaving ~tens of rows estimated
    ~5000 groups (clamped only to the full table); PG's own `estimate_num_groups`
    clamps to the filtered input, and now so do we."""
    import json

    _seed(db, n_partitions=2, rows_per_partition=50_000, high_card=5000)
    db.execute("SELECT deltax.deltax_analyze_table('events')")
    db.commit()

    # A narrow ts window (histogram-estimated) leaves ~tens of rows; uid is
    # independent of the filter, so the surviving groups can't exceed those rows
    # regardless of n_distinct(uid)=5000.
    t0, t1 = db.execute(
        "SELECT min(ts), min(ts) + interval '1 minute' FROM events"
    ).fetchone()

    plan = db.execute(
        "EXPLAIN (FORMAT JSON) SELECT uid, count(*) FROM events "
        "WHERE ts >= %s AND ts < %s GROUP BY uid",
        (t0, t1),
    ).fetchone()[0]
    root = json.loads(plan) if isinstance(plan, str) else plan

    def find_agg(node):
        if node.get("Custom Plan Provider") == "DeltaXAgg" or node.get(
            "Node Type"
        ) in ("Aggregate", "GroupAggregate", "HashAggregate"):
            return node
        for c in node.get("Plans", []) or []:
            r = find_agg(c)
            if r:
                return r
        return None

    agg = find_agg(root[0]["Plan"])
    assert agg is not None, "no aggregate node in plan"
    assert agg.get("Custom Plan Provider") == "DeltaXAgg", (
        f"expected DeltaXAgg pushdown, got {agg.get('Node Type')} — the test no "
        f"longer exercises the group-count clamp"
    )
    est = agg.get("Plan Rows", 0)
    # n_distinct(uid)=5000; the filtered input is ~tens of rows, so the clamped
    # group estimate must be far below 5000. Pre-fix it was ~5000.
    assert est < 1000, (
        f"GROUP BY uid estimate {est} not clamped to the filtered input — looks "
        f"like the raw n_distinct (5000), i.e. the clamp regressed"
    )
