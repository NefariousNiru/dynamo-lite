# file: src/plots/utils.py
from __future__ import annotations

from pathlib import Path
from typing import Optional


def latest_file_with_prefix(
    directory: Path, prefix: str, suffix: str
) -> Optional[Path]:
    """
    Return the most recently modified file in directory that matches prefix+suffix.
    """
    candidates = [
        p for p in directory.glob(f"{prefix}*{suffix}") if p.is_file()
    ]
    if not candidates:
        return None
    return max(candidates, key=lambda p: p.stat().st_mtime)
