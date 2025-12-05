# file: src/experiments/sac.py
from __future__ import annotations

import csv
import datetime as dt
import random
import secrets
import time
from pathlib import Path
from typing import Dict, List

import httpx

from src.client import DynLiteHttpClient
from src.config import CONFIG


def run_sac(clients: Dict[str, DynLiteHttpClient]) -> Path:
    """
    SAC-style experiment (client side):

    - For a moderate concurrency and keyset, send reads with random SLO classes:
        * "tight"  (deadline ~ tight_deadline_ms)
        * "relaxed" (deadline ~ relaxed_deadline_ms)
    - Attach a header X-Deadline-Ms with the chosen deadline.
    - Measure latency distribution for each class.

    Outputs:
      - sac-<ts>.csv
      - sac-<ts>.txt
    """
    cfg = CONFIG
    paths = cfg.paths
    paths.ensure_dirs()

    ts = dt.datetime.now().strftime("%Y%m%d-%H%M%S")
    txt_path = paths.raw_logs_dir / f"sac-{ts}.txt"
    csv_path = paths.raw_logs_dir / f"sac-{ts}.csv"

    cluster = cfg.cluster
    primary = cluster.primary
    node_names = [n.name for n in cluster.nodes]

    # Build a raw httpx client so we can inject headers directly.
    http_clients: Dict[str, httpx.Client] = {
        name: httpx.Client(base_url=clients[name].node.base_url, timeout=5.0)
        for name in node_names
    }

    # Populate some keys first via primary.
    num_keys = 10_000
    keys = [f"sac-{i}" for i in range(num_keys)]
    for k in keys[:2000]:
        val = f"sac-init-{k}".encode("utf-8")
        clients[primary.name].put(k, val, coord_node_id=primary.name)

    duration_sec = 15
    stop_at = time.time() + duration_sec

    slo = cfg.slo
    deadline_header = slo.deadline_header_name

    records: List[Dict[str, object]] = []

    def choose_deadline() -> tuple[str, int]:
        # Roughly 50/50 tight vs relaxed
        if random.random() < 0.5:
            return "tight", slo.tight_deadline_ms
        return "relaxed", slo.relaxed_deadline_ms

    with txt_path.open("w", encoding="utf-8") as f:
        f.write(f"SAC experiment @ {ts}\n")
        f.write(f"duration={duration_sec}s, keys={num_keys}\n")
        f.write(f"deadline_header={deadline_header}\n\n")

        while time.time() < stop_at:
            key = random.choice(keys)
            node_name = random.choice(node_names)
            cli = http_clients[node_name]

            cls, deadline = choose_deadline()
            headers = {deadline_header: str(deadline)}

            t_start = time.time()
            ok = False
            try:
                resp = cli.get(f"/kv/{key}", headers=headers)
                ok = resp.status_code in (200, 404)
            except Exception:
                ok = False
            t_end = time.time()

            rec = {
                "node": node_name,
                "class": cls,
                "deadline_ms": deadline,
                "latency_ms": (t_end - t_start) * 1000.0,
                "ok": int(ok),
                "t_start_ms": int(t_start * 1000),
                "t_end_ms": int(t_end * 1000),
            }
            records.append(rec)

        # Quick summary
        for cls in ("tight", "relaxed"):
            lat = [r["latency_ms"] for r in records if r["class"] == cls and r["ok"]]
            if not lat:
                f.write(f"{cls}: no successful requests\n")
                continue
            lat_sorted = sorted(lat)
            p50 = lat_sorted[int(0.5 * len(lat_sorted))]
            p95 = lat_sorted[int(0.95 * len(lat_sorted)) - 1]
            p99 = lat_sorted[int(0.99 * len(lat_sorted)) - 1]
            f.write(
                f"{cls}: n={len(lat)}, p50={p50:.1f} ms, p95={p95:.1f} ms, p99={p99:.1f} ms\n"
            )

    # CSV
    fieldnames = [
        "node",
        "class",
        "deadline_ms",
        "latency_ms",
        "ok",
        "t_start_ms",
        "t_end_ms",
    ]
    with csv_path.open("w", encoding="utf-8", newline="") as f_csv:
        writer = csv.DictWriter(f_csv, fieldnames=fieldnames)
        writer.writeheader()
        for r in records:
            writer.writerow(r)

    for cli in http_clients.values():
        cli.close()

    return csv_path
