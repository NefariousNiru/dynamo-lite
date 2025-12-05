# file: src/experiments/chaos.py
from __future__ import annotations

import csv
import datetime as dt
import random
import time
from pathlib import Path
from typing import Dict, List

from src.client import DynLiteHttpClient
from src.config import CONFIG


def run_chaos(clients: Dict[str, DynLiteHttpClient]) -> Path:
    """
    Chaos-ish workload:

    - Randomly pick nodes and keys.
    - Mix reads and writes.
    - Intentionally push moderate concurrency from the client side.

    This does not kill nodes automatically (you can still do manual failures
    while this runs). It mainly gives you a noisy latency trace.
    """
    cfg = CONFIG
    paths = cfg.paths
    paths.ensure_dirs()

    ts = dt.datetime.now().strftime("%Y%m%d-%H%M%S")
    txt_path = paths.raw_logs_dir / f"chaos-{ts}.txt"
    csv_path = paths.raw_logs_dir / f"chaos-{ts}.csv"

    node_names = list(clients.keys())
    num_keys = 20_000
    keys = [f"chaos-{i}" for i in range(num_keys)]
    duration_sec = 20
    stop_at = time.time() + duration_sec

    records: List[Dict[str, object]] = []

    with txt_path.open("w", encoding="utf-8") as f:
        f.write(f"Chaos experiment @ {ts}\n")
        f.write(f"duration={duration_sec}s, keys={num_keys}\n")
        f.write("You can optionally kill/restart nodes while this is running.\n\n")

        while time.time() < stop_at:
            key = random.choice(keys)
            node_name = random.choice(node_names)
            client = clients[node_name]

            op = "GET" if random.random() < 0.7 else "PUT"
            t_start = time.time()
            ok = False

            try:
                if op == "GET":
                    client.get(key)
                else:
                    value = f"chaos-{key}-{time.time_ns()}".encode("utf-8")
                    client.put(key, value, coord_node_id=node_name)
                ok = True
            except Exception as exc:  # noqa: BLE001
                ok = False
                f.write(f"ERROR op={op} node={node_name} key={key}: {exc}\n")

            t_end = time.time()
            records.append(
                {
                    "node": node_name,
                    "op": op,
                    "ok": int(ok),
                    "latency_ms": (t_end - t_start) * 1000.0,
                    "t_start_ms": int(t_start * 1000),
                    "t_end_ms": int(t_end * 1000),
                }
            )

    # CSV for potential plotting
    fieldnames = ["node", "op", "ok", "latency_ms", "t_start_ms", "t_end_ms"]
    with csv_path.open("w", encoding="utf-8", newline="") as f_csv:
        writer = csv.DictWriter(f_csv, fieldnames=fieldnames)
        writer.writeheader()
        for r in records:
            writer.writerow(r)

    return csv_path
