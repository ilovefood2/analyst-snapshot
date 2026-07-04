from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from analyst_snapshot.storage import utc_now_iso


class JsonlLogger:
    def __init__(self, logs_dir: Path, run_id: str) -> None:
        logs_dir.mkdir(parents=True, exist_ok=True)
        self.path = logs_dir / f"{run_id}.jsonl"

    def write(self, event: str, **fields: Any) -> None:
        record = {"ts": utc_now_iso(), "event": event, **fields}
        with self.path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(record, sort_keys=True, default=str) + "\n")
