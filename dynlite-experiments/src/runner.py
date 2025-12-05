# file: src/runner.py
from __future__ import annotations

import argparse
import datetime as dt
import secrets
import sys
import time
from typing import Dict

from src.client import DynLiteHttpClient, build_cluster_clients
from src.config import CONFIG
from src.experiments.anti_entropy import run_anti_entropy
from src.experiments.chaos import run_chaos
from src.experiments.functional import run_functional
from src.experiments.perf_ycsb import run_perf_ycsb
from src.experiments.sac import run_sac
from src.experiments.staleness import run_staleness
from src.plots.all_plots import generate_all_plots


def _now_iso() -> str:
    return dt.datetime.now().isoformat(timespec="seconds")


def sanity_smoke(clients: Dict[str, DynLiteHttpClient]) -> None:
    """
    Minimal end-to-end sanity check.
    """
    cfg = CONFIG
    cfg.paths.ensure_dirs()

    key_suffix = secrets.token_hex(4)
    key = f"exp-sanity-{key_suffix}"
    value = f"hello-dynlite-{key_suffix}".encode("utf-8")

    primary = cfg.cluster.primary
    primary_client = clients[primary.name]

    print(f"[{_now_iso()}] [sanity] writing key={key!r} via primary={primary.name}")
    primary_client.put(key, value, coord_node_id=primary.name)

    wait_seconds = 5
    print(f"[{_now_iso()}] [sanity] waiting {wait_seconds}s for replication/gossip...")
    time.sleep(wait_seconds)

    print(f"[{_now_iso()}] [sanity] reading key={key!r} from all nodes")
    results = {}
    for name, client in clients.items():
        try:
            r = client.get(key)
            results[name] = r
        except Exception as exc:  # noqa: BLE001
            print(f"[sanity] ERROR: GET failed on {name}: {exc}", file=sys.stderr)
            results[name] = None

    found_values = {
        name: r.value
        for name, r in results.items()
        if r is not None and r.found and r.value is not None
    }

    all_equal = len(set(found_values.values())) <= 1
    status = "CONSISTENT" if all_equal else "INCONSISTENT"

    print(f"[{_now_iso()}] [sanity] status={status}")
    for name, r in results.items():
        if r is None:
            print(f"  - {name}: ERROR (request failed)")
        elif not r.found:
            print(f"  - {name}: NOT_FOUND")
        else:
            print(f"  - {name}: FOUND len={len(r.value)} bytes, clock={r.clock}")


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        description="DynLite++ experiment runner (Python harness)",
    )
    parser.add_argument(
        "--experiment",
        "-e",
        default="all",
        help=(
            "Which experiment to run. "
            "Options: sanity, functional, perf, sac, chaos, staleness, anti-entropy, all "
            "(default: all)."
        ),
    )

    args = parser.parse_args(argv)

    clients = build_cluster_clients()

    try:
        exp = args.experiment.lower()
        if exp in ("sanity", "sanity-smoke"):
            sanity_smoke(clients)

        elif exp == "functional":
            run_functional(clients)

        elif exp in ("perf", "perf-ycsb"):
            run_perf_ycsb(clients)

        elif exp == "sac":
            run_sac(clients)

        elif exp == "chaos":
            run_chaos(clients)

        elif exp == "staleness":
            run_staleness(clients)

        elif exp in ("anti-entropy", "anti_entropy"):
            run_anti_entropy(clients)

        elif exp == "plot":
            generate_all_plots()

        elif exp == "all":
            print(f"[{_now_iso()}] Running sanity...")
            sanity_smoke(clients)
            print(f"[{_now_iso()}] Running functional tests...")
            run_functional(clients)
            print(f"[{_now_iso()}] Running perf/YCSB...")
            run_perf_ycsb(clients)
            print(f"[{_now_iso()}] Running SAC client-side SLO experiment...")
            run_sac(clients)
            print(f"[{_now_iso()}] Running chaos workload...")
            run_chaos(clients)
            print(f"[{_now_iso()}] Running staleness experiment...")
            run_staleness(clients)
            print(f"[{_now_iso()}] Running anti-entropy convergence experiment...")
            run_anti_entropy(clients)

            print(f"[{_now_iso()}] Generating plots...")
            generate_all_plots()
            print(f"[{_now_iso()}] All experiments + plots complete.")

        else:
            print(f"Unknown experiment: {args.experiment!r}", file=sys.stderr)
            sys.exit(1)
    finally:
        for c in clients.values():
            c.close()


if __name__ == "__main__":
    main()
