from pathlib import Path
import json
import os
from typing import List, Any


def read_json(path: Path, default):
    if not path.exists() or path.stat().st_size == 0:
        return default

    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def write_json(path: Path, data) -> None:
    """Write JSON atomically so a crash can't leave a half-written file."""
    tmp_path = path.with_suffix(path.suffix + ".tmp")

    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=4, ensure_ascii=False)

    os.replace(tmp_path, path)


def split_batch(data: List[Any], batch_size: int) -> List[List[Any]]:
    """Splits a list into smaller lists of the specified size."""
    if batch_size <= 0:
        raise ValueError("Batch size must be greater than 0.")

    return [data[i : i + batch_size] for i in range(0, len(data), batch_size)]