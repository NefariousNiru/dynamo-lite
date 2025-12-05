# file: src/config.py
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Tuple


# ----------------- cluster config -----------------


@dataclass(frozen=True)
class NodeConfig:
    """
    Configuration for a single DynLite node.

    Example:
        NodeConfig(name="node-a", base_url="http://localhost:8080")
    """
    name: str
    base_url: str  # e.g. "http://localhost:8080"


@dataclass(frozen=True)
class ClusterConfig:
    """
    Logical view of the DynLite cluster from the Python side.
    """
    nodes: List[NodeConfig]

    @property
    def primary(self) -> NodeConfig:
        if not self.nodes:
            raise RuntimeError("ClusterConfig.nodes is empty")
        return self.nodes[0]

    def by_name(self, name: str) -> NodeConfig:
        for n in self.nodes:
            if n.name == name:
                return n
        raise KeyError(f"No node with name={name!r} in ClusterConfig")


# Default: 3-node localhost cluster as you are running now.
DEFAULT_CLUSTER = ClusterConfig(
    nodes=[
        NodeConfig(name="node-a", base_url="http://localhost:8080"),
        NodeConfig(name="node-b", base_url="http://localhost:8081"),
        NodeConfig(name="node-c", base_url="http://localhost:8082"),
    ]
)


# ----------------- workload / experiment knobs -----------------


@dataclass(frozen=True)
class WorkloadConfig:
    """
    Parameters for YCSB-style workloads.

    Defaults are intentionally conservative for a 16 GB laptop.
    You can bump these once you see how it behaves.
    """
    num_keys: int = 1_000_000
    value_size_bytes: int = 256
    warmup_seconds: int = 5
    run_seconds: int = 20
    read_write_mix_a: Tuple[float, float] = (0.5, 0.5)    # Workload A: 50/50
    read_write_mix_b: Tuple[float, float] = (0.95, 0.05)  # Workload B: 95/5
    read_write_mix_c: Tuple[float, float] = (1.0, 0.0)    # Workload C: 100% read
    concurrency_levels: Tuple[int, ...] = (4, 8, 32, 64, 128, 256)     # keep moderate


@dataclass(frozen=True)
class SloConfig:
    """
    Client-side SLO buckets.

    Even if the server's SAC is not fully wired, we can still label requests
    as "tight" vs "relaxed" and measure the resulting latencies.
    """
    tight_deadline_ms: int = 20
    relaxed_deadline_ms: int = 100
    deadline_header_name: str = "X-Deadline-Ms"


@dataclass(frozen=True)
class StalenessConfig:
    """
    Knobs for staleness experiments.
    """
    delays_ms: Tuple[int, ...] = (0, 10, 20, 50, 100, 200, 500, 1000)
    trials_per_delay: int = 50


@dataclass(frozen=True)
class AntiEntropyConfig:
    """
    Knobs for anti-entropy / Merkle snapshot experiments.
    """
    observation_seconds: int = 30
    sample_interval_seconds: int = 1
    leaf_count: int = 1024
    write_burst_keys: int = 200  # keys to write before observing


@dataclass(frozen=True)
class PathsConfig:
    """
    Output locations for experiment artifacts.
    """
    base_dir: Path = Path("results")
    raw_logs_dir: Path = Path("results") / "raw_logs"
    summary_path: Path = Path("results") / "summary.txt"
    figures_dir: Path = Path("results") / "figures"

    def ensure_dirs(self) -> None:
        self.base_dir.mkdir(parents=True, exist_ok=True)
        self.raw_logs_dir.mkdir(parents=True, exist_ok=True)
        self.figures_dir.mkdir(parents=True, exist_ok=True)


@dataclass(frozen=True)
class ExperimentConfig:
    """
    Top-level configuration bundle consumed by runner + experiments.
    """
    cluster: ClusterConfig = DEFAULT_CLUSTER
    workload: WorkloadConfig = WorkloadConfig()
    slo: SloConfig = SloConfig()
    staleness: StalenessConfig = StalenessConfig()
    anti_entropy: AntiEntropyConfig = AntiEntropyConfig()
    paths: PathsConfig = PathsConfig()


CONFIG = ExperimentConfig()
