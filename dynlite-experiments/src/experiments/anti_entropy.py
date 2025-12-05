# file: src/experiments/anti_entropy.py
from __future__ import annotations

import csv
import datetime as dt
import time
from pathlib import Path
from typing import Dict, List

from src.client import DynLiteHttpClient
from src.config import CONFIG


def run_anti_entropy(clients: Dict[str, DynLiteHttpClient]) -> Path:
    """
    Observe Merkle roots across nodes over time and track convergence.

    Steps:
      - Issue a burst of writes on primary to perturb the cluster.
      - For observation_seconds, every sample_interval:
          * fetch Merkle snapshot from each node
          * compute root equality across pairs
    """
    cfg = CONFIG
    paths = cfg.paths
    paths.ensure_dirs()

    ts = dt.datetime.now().strftime("%Y%m%d-%H%M%S")
    csv_path = paths.raw_logs_dir / f"anti_entropy-{ts}.csv"
    txt_path = paths.raw_logs_dir / f"anti_entropy-{ts}.txt"

    ae_cfg = cfg.anti_entropy
    primary = cfg.cluster.primary
    primary_client = clients[primary.name]
    node_names = list(clients.keys())

    # Burst of writes
    with txt_path.open("w", encoding="utf-8") as f:
        f.write(f"Anti-entropy experiment @ {ts}\n")
        f.write(
            f"write_burst_keys={ae_cfg.write_burst_keys}, "
            f"observation_seconds={ae_cfg.observation_seconds}, "
            f"sample_interval={ae_cfg.sample_interval_seconds}s\n\n"
        )

        f.write("Write burst on primary...\n")
        for i in range(ae_cfg.write_burst_keys):
            key = f"ae-{i}"
            value = f"ae-val-{i}-{time.time_ns()}".encode("utf-8")
            primary_client.put(key, value, coord_node_id=primary.name)
        f.write("Write burst done.\n\n")

    # Observation
    records: List[Dict[str, object]] = []
    start_time = time.time()
    end_time = start_time + ae_cfg.observation_seconds

    while time.time() < end_time:
        t_now = time.time()
        t_offset = t_now - start_time

        # snapshot per node
        roots: Dict[str, str] = {}
        for name in node_names:
            snap = clients[name].fetch_merkle_snapshot(
                leaf_count=ae_cfg.leaf_count
            )
            roots[name] = snap.get("rootHashBase64", "")

        # pairwise equality
        for i in range(len(node_names)):
            for j in range(i + 1, len(node_names)):
                n1 = node_names[i]
                n2 = node_names[j]
                in_sync = int(roots[n1] == roots[n2])
                records.append(
                    {
                        "t_offset_s": t_offset,
                        "node_a": n1,
                        "node_b": n2,
                        "root_a": roots[n1],
                        "root_b": roots[n2],
                        "in_sync": in_sync,
                    }
                )

        time.sleep(ae_cfg.sample_interval_seconds)

    # CSV
    fieldnames = [
        "t_offset_s",
        "node_a",
        "node_b",
        "root_a",
        "root_b",
        "in_sync",
    ]
    with csv_path.open("w", encoding="utf-8", newline="") as f_csv:
        writer = csv.DictWriter(f_csv, fieldnames=fieldnames)
        writer.writeheader()
        for r in records:
            writer.writerow(r)

    return csv_path
