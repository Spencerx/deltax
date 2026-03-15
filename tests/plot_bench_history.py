#!/usr/bin/env python3
"""Plot benchmark progress over time from history directories."""

import json
import math
import os
from datetime import datetime
from pathlib import Path

import plotly.graph_objects as go

HISTORY_DIR = Path(__file__).parent / ".bench_results" / "history"
OUTPUT_FILE = Path(__file__).parent / ".bench_results" / "bench_progress.html"


def geomean(values):
    """Compute geometric mean of a list of positive numbers."""
    valid = [v for v in values if v is not None and v > 0]
    if not valid:
        return None
    return math.exp(sum(math.log(v) for v in valid) / len(valid))


def parse_dir_name(name):
    """Parse 'YYYYMMDD_HHMMSS_hash' → (datetime, hash)."""
    parts = name.split("_")
    dt = datetime.strptime(f"{parts[0]}_{parts[1]}", "%Y%m%d_%H%M%S")
    commit = parts[2] if len(parts) > 2 else ""
    return dt, commit


def load_history():
    dirs = sorted(d for d in os.listdir(HISTORY_DIR) if (HISTORY_DIR / d).is_dir())

    results = []
    for d in dirs:
        dt, commit = parse_dir_name(d)
        entry = {"datetime": dt, "commit": commit, "dir": d}

        st_path = HISTORY_DIR / d / "pg_deltax.json"
        if st_path.exists():
            data = json.loads(st_path.read_text())
            comp = list(data.get("compressed_queries", {}).values())
            uncomp = list(data.get("uncompressed_queries", {}).values())
            entry["deltax_geomean"] = geomean(comp)
            entry["deltax_n"] = len(comp)
            entry["postgres_geomean"] = geomean(uncomp)
            entry["postgres_n"] = len(uncomp)

        ts_path = HISTORY_DIR / d / "timescaledb_tsl.json"
        if ts_path.exists():
            data = json.loads(ts_path.read_text())
            comp = list(data.get("compressed_queries", {}).values())
            entry["timescale_geomean"] = geomean(comp)
            entry["timescale_n"] = len(comp)

        results.append(entry)

    return results


def build_annotations(dates, counts, label):
    """Find points where query count changes and return annotation texts."""
    annotations = []
    prev = None
    for i, n in enumerate(counts):
        if n is not None and n != prev:
            if prev is not None:
                annotations.append((dates[i], label, prev, n))
            prev = n
    return annotations


def main():
    history = load_history()

    # DeltaX
    st_dates = [e["datetime"] for e in history if e.get("deltax_geomean")]
    st_vals = [e["deltax_geomean"] for e in history if e.get("deltax_geomean")]
    st_n = [e["deltax_n"] for e in history if e.get("deltax_geomean")]
    st_commits = [e["commit"] for e in history if e.get("deltax_geomean")]

    # Postgres
    pg_dates = [e["datetime"] for e in history if e.get("postgres_geomean")]
    pg_vals = [e["postgres_geomean"] for e in history if e.get("postgres_geomean")]
    pg_n = [e["postgres_n"] for e in history if e.get("postgres_geomean")]
    pg_commits = [e["commit"] for e in history if e.get("postgres_geomean")]

    # TimescaleDB
    ts_dates = [e["datetime"] for e in history if e.get("timescale_geomean")]
    ts_vals = [e["timescale_geomean"] for e in history if e.get("timescale_geomean")]
    ts_n = [e["timescale_n"] for e in history if e.get("timescale_geomean")]
    ts_commits = [e["commit"] for e in history if e.get("timescale_geomean")]

    fig = go.Figure()

    def hover_text(dates, commits, vals, ns):
        return [
            f"{dt:%Y-%m-%d %H:%M}<br>commit: {c}<br>geomean: {v:.1f} ms<br>queries: {n}"
            for dt, c, v, n in zip(dates, commits, vals, ns)
        ]

    fig.add_trace(go.Scatter(
        x=st_dates, y=st_vals, mode="lines+markers", name="DeltaX (compressed)",
        line=dict(color="#2ecc71", width=2), marker=dict(size=4),
        hovertext=hover_text(st_dates, st_commits, st_vals, st_n), hoverinfo="text",
    ))
    fig.add_trace(go.Scatter(
        x=pg_dates, y=pg_vals, mode="lines+markers", name="Postgres (uncompressed)",
        line=dict(color="#3498db", width=2), marker=dict(size=4),
        hovertext=hover_text(pg_dates, pg_commits, pg_vals, pg_n), hoverinfo="text",
    ))
    fig.add_trace(go.Scatter(
        x=ts_dates, y=ts_vals, mode="lines+markers", name="TimescaleDB TSL (compressed)",
        line=dict(color="#e74c3c", width=2), marker=dict(size=4),
        hovertext=hover_text(ts_dates, ts_commits, ts_vals, ts_n), hoverinfo="text",
    ))

    # Annotate query count changes
    all_annotations = []
    all_annotations.extend(build_annotations(st_dates, st_n, "ST"))
    all_annotations.extend(build_annotations(pg_dates, pg_n, "PG"))
    all_annotations.extend(build_annotations(ts_dates, ts_n, "TS"))

    # Deduplicate by (date, old→new) since ST and PG usually change together
    seen = set()
    for dt, label, old, new in all_annotations:
        key = (dt, old, new)
        if key not in seen:
            seen.add(key)
            fig.add_annotation(
                x=dt, y=0, yref="paper", yshift=10,
                text=f"{old}→{new}q", showarrow=False,
                font=dict(size=9, color="gray"),
            )

    fig.update_layout(
        title="ClickBench Progress",
        xaxis_title="Date",
        yaxis_title="Geometric Mean (ms)",
        yaxis_type="log",
        yaxis_tickvals=[10, 20, 30, 50, 75, 100, 150, 200, 300, 500, 750, 1000],
        yaxis_ticktext=["10", "20", "30", "50", "75", "100", "150", "200", "300", "500", "750", "1000"],
        template="plotly_white",
        hovermode="x unified",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    )

    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    fig.write_html(str(OUTPUT_FILE))
    print(f"Saved to {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
