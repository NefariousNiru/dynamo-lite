# file: src/experiments/staleness.py
from __future__ import annotations

import csv
import datetime as dt
import random
import secrets
import time
from pathlib import Path
from typing import Dict, List

from src.client import DynLiteHttpClient
from src.config import CONFIG


def run_staleness(clients: Dict[str, DynLiteHttpClient]) -> Path:
    """
    Measure stale read probability as a function of read-after-write delay.

    For each delay d:
      - write a new value for a key on primary
      - sleep d ms
      - read from a random node
      - label as stale if the value does not match the latest write
    """
    cfg = CONFIG
    paths = cfg.paths
    paths.ensure_dirs()

    ts = dt.datetime.now().strftime("%Y%m%d-%H%M%S")
    csv_path = paths.raw_logs_dir / f"staleness-{ts}.csv"
    txt_path = paths.raw_logs_dir / f"staleness-{ts}.txt"

    primary = cfg.cluster.primary
    primary_client = clients[primary.name]
    node_names = list(clients.keys())

    delays = cfg.staleness.delays_ms
    trials = cfg.staleness.trials_per_delay

    key = f"stale-{secrets.token_hex(4)}"

    records: List[Dict[str, object]] = []

    with txt_path.open("w", encoding="utf-8") as f:
        f.write(f"Staleness experiment @ {ts}\n")
        f.write(f"key={key}\n")
        f.write(f"delays_ms={delays}, trials_per_delay={trials}\n\n")

        for d in delays:
            stale_count = 0
            total = 0
            for _ in range(trials):
                total += 1
                expected_value = f"v-{d}-{time.time_ns()}".encode("utf-8")
                primary_client.put(key, expected_value, coord_node_id=primary.name)

                time.sleep(d / 1000.0)

                read_node = random.choice(node_names)
                r = clients[read_node].get(key)

                is_stale = 1
                if r.found and r.value == expected_value:
                    is_stale = 0
                elif not r.found:
                    is_stale = 1

                if is_stale:
                    stale_count += 1

                records.append(
                    {
                        "delay_ms": d,
                        "read_node": read_node,
                        "is_stale": is_stale,
                    }
                )

            frac = stale_count / max(total, 1)
            f.write(f"delay={d} ms: stale={stale_count}/{total} ({frac:.2%})\n")

    # CSV
    fieldnames = ["delay_ms", "read_node", "is_stale"]
    with csv_path.open("w", encoding="utf-8", newline="") as f_csv:
        writer = csv.DictWriter(f_csv, fieldnames=fieldnames)
        writer.writeheader()
        for r in records:
            writer.writerow(r)

    return csv_path
