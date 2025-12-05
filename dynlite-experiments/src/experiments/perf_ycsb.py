# file: src/experiments/perf_ycsb.py
from __future__ import annotations

import csv
import datetime as dt
import random
import secrets
import string
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Dict, List, Tuple

from src.client import DynLiteHttpClient
from src.config import CONFIG


def _rand_value(size: int) -> bytes:
    alphabet = string.ascii_letters + string.digits
    s = "".join(random.choice(alphabet) for _ in range(size))
    return s.encode("utf-8")


def _choose_op(mix: Tuple[float, float]) -> str:
    p_read, p_write = mix
    x = random.random()
    return "GET" if x < p_read else "PUT"


def run_perf_ycsb(clients: Dict[str, DynLiteHttpClient]) -> Tuple[Path, Path]:
    """
    Run YCSB-style workloads (A, B, C) at different concurrency levels.

    Outputs:
      - perf-<ts>.txt : human-readable summary
      - perf-<ts>.csv : per-request records for plotting
    """
    cfg = CONFIG
    wl = cfg.workload
    paths = cfg.paths
    paths.ensure_dirs()

    ts = dt.datetime.now().strftime("%Y%m%d-%H%M%S")
    txt_path = paths.raw_logs_dir / f"perf-{ts}.txt"
    csv_path = paths.raw_logs_dir / f"perf-{ts}.csv"

    primary = cfg.cluster.primary
    primary_client = clients[primary.name]

    # Pre-generate key space used by all workloads.
    keys = [f"perf-{i}" for i in range(wl.num_keys)]

    workloads = [
        ("A", wl.read_write_mix_a),
        ("B", wl.read_write_mix_b),
        ("C", wl.read_write_mix_c),
    ]

    records: List[Dict[str, object]] = []

    with txt_path.open("w", encoding="utf-8") as f:
        f.write(f"Perf/YCSB @ {ts}\n")
        f.write(f"Nodes: {[n.name for n in cfg.cluster.nodes]}\n")
        f.write(f"num_keys={wl.num_keys}, value_size={wl.value_size_bytes} bytes\n")
        f.write(f"concurrency_levels={wl.concurrency_levels}\n\n")

        # Warmup: simple write pass (no logging)
        f.write("Warmup phase (writes)...\n")
        for i in range(min(1_000, wl.num_keys)):
            k = keys[i]
            v = _rand_value(wl.value_size_bytes)
            primary_client.put(k, v, coord_node_id=primary.name)
        f.write("Warmup done.\n\n")

        for workload_name, mix in workloads:
            f.write(f"=== Workload {workload_name} (read/write={mix}) ===\n")
            for conc in wl.concurrency_levels:
                f.write(f"  - Running with concurrency={conc}\n")
                f.flush()

                # Per workload+concurrency run
                run_records = _run_single_workload(
                    workload_name=workload_name,
                    mix=mix,
                    concurrency=conc,
                    duration_sec=wl.run_seconds,
                    keys=keys,
                    clients=clients,
                    value_size=wl.value_size_bytes,
                )
                records.extend(run_records)

                # Compute throughput and percentiles for summary
                latencies = [r["latency_ms"] for r in run_records if r["ok"]]
                if not latencies:
                    f.write("    No successful requests.\n")
                    continue

                duration_ms = max(r["t_end_ms"] for r in run_records) - min(
                    r["t_start_ms"] for r in run_records
                )
                duration_s = max(duration_ms / 1000.0, 1e-6)
                throughput = len(run_records) / duration_s

                latencies_sorted = sorted(latencies)
                p50 = latencies_sorted[int(0.5 * len(latencies_sorted))]
                p95 = latencies_sorted[int(0.95 * len(latencies_sorted)) - 1]
                p99 = latencies_sorted[int(0.99 * len(latencies_sorted)) - 1]

                f.write(
                    f"    ops={len(run_records)}, throughput={throughput:.1f} ops/s, "
                    f"p50={p50:.1f} ms, p95={p95:.1f} ms, p99={p99:.1f} ms\n"
                )
            f.write("\n")

    # Write CSV for plotting
    fieldnames = [
        "workload",
        "concurrency",
        "node",
        "op",
        "ok",
        "latency_ms",
        "t_start_ms",
        "t_end_ms",
    ]
    with csv_path.open("w", encoding="utf-8", newline="") as f_csv:
        writer = csv.DictWriter(f_csv, fieldnames=fieldnames)
        writer.writeheader()
        for rec in records:
            writer.writerow(rec)

    return txt_path, csv_path


def _run_single_workload(
    workload_name: str,
    mix: Tuple[float, float],
    concurrency: int,
    duration_sec: int,
    keys: List[str],
    clients: Dict[str, DynLiteHttpClient],
    value_size: int,
) -> List[Dict[str, object]]:
    """
    Run one workload for duration_sec with a given concurrency.

    Returns a list of per-request records.
    """
    stop_at = time.time() + duration_sec
    records: List[Dict[str, object]] = []

    node_names = list(clients.keys())

    def worker_thread(thread_id: int) -> None:
        nonlocal records
        random.seed(secrets.token_bytes(16))

        while time.time() < stop_at:
            op = _choose_op(mix)
            key = random.choice(keys)
            node_name = random.choice(node_names)
            client = clients[node_name]

            t_start = time.time()
            ok = False
            try:
                if op == "GET":
                    client.get(key)
                else:
                    client.put(key, _rand_value(value_size), coord_node_id=node_name)
                ok = True
            except Exception:
                ok = False
            t_end = time.time()

            rec = {
                "workload": workload_name,
                "concurrency": concurrency,
                "node": node_name,
                "op": op,
                "ok": int(ok),
                "latency_ms": (t_end - t_start) * 1000.0,
                "t_start_ms": int(t_start * 1000),
                "t_end_ms": int(t_end * 1000),
            }
            records.append(rec)

    with ThreadPoolExecutor(max_workers=concurrency) as ex:
        futures = [ex.submit(worker_thread, i) for i in range(concurrency)]
        for _ in as_completed(futures):
            # Nothing to inspect; join all threads.
            pass

    return records
