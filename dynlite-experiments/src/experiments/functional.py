# file: src/experiments/functional.py
from __future__ import annotations

import datetime as dt
import secrets
from pathlib import Path
from typing import Dict

from src.client import DynLiteHttpClient
from src.config import CONFIG


def run_functional(clients: Dict[str, DynLiteHttpClient]) -> Path:
    """
    Very lightweight functional test suite:
      - write a key on primary
      - verify read-your-writes on primary
      - verify delete semantics
    """
    cfg = CONFIG
    paths = cfg.paths
    paths.ensure_dirs()

    ts = dt.datetime.now().strftime("%Y%m%d-%H%M%S")
    log_path = paths.raw_logs_dir / f"functional-{ts}.txt"

    primary = cfg.cluster.primary
    primary_client = clients[primary.name]

    key = f"func-{secrets.token_hex(4)}"
    value = b"functional-test-value"

    with log_path.open("w", encoding="utf-8") as f:
        f.write(f"Functional tests @ {ts}\n")
        f.write(f"Primary node: {primary.name}\n")
        f.write(f"Key: {key}\n\n")

        # Put
        f.write("1) PUT then GET on primary\n")
        primary_client.put(key, value, coord_node_id=primary.name)
        r = primary_client.get(key)
        f.write(f"   GET found={r.found}, value={r.value!r}\n")
        ok1 = r.found and r.value == value
        f.write(f"   PASS={ok1}\n\n")

        # Delete
        f.write("2) DELETE then GET on primary\n")
        primary_client.delete(key, coord_node_id=primary.name)
        r2 = primary_client.get(key)
        f.write(f"   GET after delete found={r2.found}\n")
        ok2 = not r2.found
        f.write(f"   PASS={ok2}\n\n")

        f.write(f"Overall PASS={ok1 and ok2}\n")

    return log_path
