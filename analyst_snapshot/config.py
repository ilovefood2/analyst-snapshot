from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv


@dataclass(frozen=True)
class Config:
    snapshot_dir: Path
    universe_file: Path
    logs_dir: Path
    symbol_delay_seconds: float
    dropbox_remote_root: str


def load_config() -> Config:
    load_dotenv()
    return Config(
        snapshot_dir=Path(os.getenv("SNAPSHOT_DIR", "./archive")),
        universe_file=Path(os.getenv("UNIVERSE_FILE", "./universe.txt")),
        logs_dir=Path(os.getenv("LOG_DIR", "./logs")),
        symbol_delay_seconds=float(os.getenv("SYMBOL_DELAY_SECONDS", "0.5")),
        dropbox_remote_root=os.getenv("DROPBOX_REMOTE_ROOT", "/DailyStockSnapshots"),
    )
