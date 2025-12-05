# file: src/plots/all_plots.py
from __future__ import annotations

import csv
from pathlib import Path
from typing import Dict, List, Tuple

import plotly.express as px

from src.config import CONFIG
from src.plots.utils import latest_file_with_prefix


def generate_all_plots() -> None:
    paths = CONFIG.paths
    paths.ensure_dirs()

    raw_dir = paths.raw_logs_dir
    fig_dir = paths.figures_dir

    fig_dir.mkdir(parents=True, exist_ok=True)

    perf_csv = latest_file_with_prefix(raw_dir, "perf-", ".csv")
    if perf_csv:
        _plot_perf(perf_csv, fig_dir)

    staleness_csv = latest_file_with_prefix(raw_dir, "staleness-", ".csv")
    if staleness_csv:
        _plot_staleness(staleness_csv, fig_dir)

    ae_csv = latest_file_with_prefix(raw_dir, "anti_entropy-", ".csv")
    if ae_csv:
        _plot_anti_entropy(ae_csv, fig_dir)

    chaos_csv = latest_file_with_prefix(raw_dir, "chaos-", ".csv")
    if chaos_csv:
        _plot_chaos(chaos_csv, fig_dir)

    sac_csv = latest_file_with_prefix(raw_dir, "sac-", ".csv")
    if sac_csv:
        _plot_sac(sac_csv, fig_dir)


def _plot_perf(csv_path: Path, fig_dir: Path) -> None:
    # Load CSV
    rows: List[Dict[str, str]] = []
    with csv_path.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows.extend(reader)

    # Throughput vs concurrency per workload
    # Aggregate: count ops / time window
    by_key: Dict[Tuple[str, int], List[float]] = {}
    for r in rows:
        if r["ok"] != "1":
            continue
        workload = r["workload"]
        conc = int(r["concurrency"])
        key = (workload, conc)
        by_key.setdefault(key, []).append(float(r["latency_ms"]))

    data_tp = []
    for (workload, conc), lats in by_key.items():
        # Rough throughput: ops / (avg_latency * conc) is too hand-wavy,
        # instead we treat the run as roughly of same duration for each pair.
        # Use ops per second approximated by ops / 20s (run_seconds default).
        duration = float(CONFIG.workload.run_seconds)
        throughput = len(lats) / duration
        data_tp.append(
            {
                "workload": workload,
                "concurrency": conc,
                "throughput_ops_s": throughput,
            }
        )

    if data_tp:
        fig = px.line(
            data_tp,
            x="concurrency",
            y="throughput_ops_s",
            color="workload",
            markers=True,
            title="Throughput vs Concurrency (per workload)",
        )
        fig.write_image(str(fig_dir / "perf_throughput.png"))
        fig.write_html(str(fig_dir / "perf_throughput.html"))

    # Latency percentiles vs concurrency
    from math import floor

    data_lat = []
    grouped: Dict[Tuple[str, int], List[float]] = {}
    for r in rows:
        if r["ok"] != "1":
            continue
        workload = r["workload"]
        conc = int(r["concurrency"])
        key = (workload, conc)
        grouped.setdefault(key, []).append(float(r["latency_ms"]))

    for (workload, conc), lats in grouped.items():
        lats_sorted = sorted(lats)
        n = len(lats_sorted)
        if n == 0:
            continue

        def pct(p: float) -> float:
            idx = max(0, min(n - 1, floor(p * n) - 1))
            return lats_sorted[idx]

        p50 = pct(0.50)
        p95 = pct(0.95)
        p99 = pct(0.99)

        data_lat.extend(
            [
                {
                    "workload": workload,
                    "concurrency": conc,
                    "percentile": "p50",
                    "latency_ms": p50,
                },
                {
                    "workload": workload,
                    "concurrency": conc,
                    "percentile": "p95",
                    "latency_ms": p95,
                },
                {
                    "workload": workload,
                    "concurrency": conc,
                    "percentile": "p99",
                    "latency_ms": p99,
                },
            ]
        )

    if data_lat:
        fig2 = px.line(
            data_lat,
            x="concurrency",
            y="latency_ms",
            color="percentile",
            facet_col="workload",
            facet_col_wrap=1,
            markers=True,
            title="Latency percentiles vs Concurrency",
        )
        fig2.update_yaxes(matches=None)
        fig2.write_image(str(fig_dir / "perf_latency.png"))
        fig2.write_html(str(fig_dir / "perf_latency.html"))


def _plot_staleness(csv_path: Path, fig_dir: Path) -> None:
    rows: List[Dict[str, str]] = []
    with csv_path.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows.extend(reader)

    by_delay: Dict[int, List[int]] = {}
    for r in rows:
        d = int(r["delay_ms"])
        is_stale = int(r["is_stale"])
        by_delay.setdefault(d, []).append(is_stale)

    data = []
    for d, vals in sorted(by_delay.items()):
        if not vals:
            continue
        frac = sum(vals) / len(vals)
        data.append({"delay_ms": d, "stale_fraction": frac})

    if not data:
        return

    fig = px.line(
        data,
        x="delay_ms",
        y="stale_fraction",
        markers=True,
        title="Stale read probability vs read-after-write delay",
    )
    fig.update_yaxes(range=[0, 1])
    fig.write_image(str(fig_dir / "staleness.png"))
    fig.write_html(str(fig_dir / "staleness.html"))


def _plot_anti_entropy(csv_path: Path, fig_dir: Path) -> None:
    rows: List[Dict[str, str]] = []
    with csv_path.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows.extend(reader)

    data = [
        {
            "t_offset_s": float(r["t_offset_s"]),
            "pair": f"{r['node_a']}-{r['node_b']}",
            "in_sync": int(r["in_sync"]),
        }
        for r in rows
    ]

    if not data:
        return

    fig = px.line(
        data,
        x="t_offset_s",
        y="in_sync",
        color="pair",
        markers=True,
        title="Anti-entropy convergence (pairwise IN_SYNC over time)",
    )
    fig.update_yaxes(range=[-0.1, 1.1])
    fig.write_image(str(fig_dir / "anti_entropy.png"))
    fig.write_html(str(fig_dir / "anti_entropy.html"))


def _plot_chaos(csv_path: Path, fig_dir: Path) -> None:
    rows: List[Dict[str, str]] = []
    with csv_path.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows.extend(reader)

    data = [
        {
            "t_start_ms": int(r["t_start_ms"]),
            "latency_ms": float(r["latency_ms"]),
            "node": r["node"],
            "op": r["op"],
        }
        for r in rows
        if r.get("ok") == "1"
    ]

    if not data:
        return

    # Normalize time to seconds from start
    t0 = min(d["t_start_ms"] for d in data)
    for d in data:
        d["t_rel_s"] = (d["t_start_ms"] - t0) / 1000.0

    fig = px.scatter(
        data,
        x="t_rel_s",
        y="latency_ms",
        color="op",
        symbol="node",
        title="Chaos workload: latency over time",
    )
    fig.write_image(str(fig_dir / "chaos_latency.png"))
    fig.write_html(str(fig_dir / "chaos_latency.html"))


def _plot_sac(csv_path: Path, fig_dir: Path) -> None:
    rows: List[Dict[str, str]] = []
    with csv_path.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows.extend(reader)

    data = [
        {
            "class": r["class"],
            "latency_ms": float(r["latency_ms"]),
        }
        for r in rows
        if r.get("ok") == "1"
    ]

    if not data:
        return

    fig = px.box(
        data,
        x="class",
        y="latency_ms",
        title="Latency distribution by SLO class (tight vs relaxed)",
    )
    fig.write_image(str(fig_dir / "sac_latency.png"))
    fig.write_html(str(fig_dir / "sac_latency.html"))
