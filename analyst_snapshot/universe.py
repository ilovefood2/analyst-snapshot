"""Build the snapshot universe from the NASDAQ stock screener.

The screener is the same source behind the NASDAQ Trader symbol directory, and it carries the
market cap and listing metadata needed to drop the long tail of securities that have no analyst
coverage to snapshot: warrants, rights, units, preferreds and trust issues.

Symbols are normalised to Yahoo's spelling. NASDAQ writes class shares as ``BRK/B`` and many
sources write ``BRK.B``; Yahoo answers only to ``BRK-B``, and asking it for the wrong spelling
returns an empty response that is indistinguishable from "no analyst coverage".
"""

from __future__ import annotations

import json
import re
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Any

SCREENER_URL = (
    "https://api.nasdaq.com/api/screener/stocks?tableonly=true&limit=25000&exchange={exchange}"
)
# The screener rejects the bare urllib user agent.
USER_AGENT = "analyst-snapshot/0.2 (research universe builder)"
DEFAULT_EXCHANGES = ("nasdaq", "nyse")
DEFAULT_MIN_MARKET_CAP = 300_000_000.0

# Securities that never carry analyst estimates or ratings. This is a blacklist rather than a
# whitelist because most common stocks are listed under a bare corporate name — "Visa Inc.",
# "Berkshire Hathaway Inc." — with no security-type suffix to match on. Note that "Depositary
# Shares" is not excluded: preferred depositary issues always also say "Preferred", while ADRs
# and ADSs are ordinary equity and must be kept.
_EXCLUDED_NAME = re.compile(
    r"\b(warrants?|rights?|units?|preferred|debenture|notes? due|contingent value"
    r"|when[- ]issued|subordinated notes?)\b|%\s*(notes?|debentures?)",
    re.IGNORECASE,
)


@dataclass(frozen=True)
class UniverseStats:
    listings: int
    after_security_filter: int
    after_market_cap_filter: int
    carried_over: int
    total: int


def normalize_symbol(symbol: str) -> str:
    """Convert a listing symbol to the spelling Yahoo answers to."""
    cleaned = symbol.strip().upper()
    return cleaned.replace("/", "-").replace(".", "-")


def is_tradeable_security(name: str) -> bool:
    """True for ordinary equity and ADRs; False for warrants, rights, units and preferreds."""
    if not name:
        return False
    return not _EXCLUDED_NAME.search(name)


def _is_plain_symbol(symbol: Any) -> bool:
    """Reject the preferred-class and when-issued spellings NASDAQ marks with ^ or a space."""
    if not symbol or not isinstance(symbol, str):
        return False
    text = symbol.strip()
    return bool(text) and "^" not in text and " " not in text


def market_cap(row: dict[str, Any]) -> float:
    raw = row.get("marketCap")
    if raw in (None, "", "0.00"):
        return 0.0
    try:
        return float(str(raw).replace(",", "").replace("$", ""))
    except ValueError:
        return 0.0


def select_symbols(
    rows: list[dict[str, Any]],
    min_market_cap: float = DEFAULT_MIN_MARKET_CAP,
    carry_over: list[str] | None = None,
) -> tuple[list[str], UniverseStats]:
    """Pick the universe from screener rows, keeping every symbol already being archived.

    Symbols are never dropped just because they fell below the cap floor or off an exchange list.
    A symbol removed from the universe stops accruing history, and point-in-time analyst data
    cannot be backfilled afterwards, so shrinking is destructive in a way that growing is not.
    """
    tradeable = [row for row in rows if is_tradeable_security(str(row.get("name", "")))]
    selected = {
        normalize_symbol(str(row["symbol"]))
        for row in tradeable
        if _is_plain_symbol(row.get("symbol")) and market_cap(row) >= min_market_cap
    }
    selected.discard("")
    above_floor = len(selected)

    existing = {normalize_symbol(symbol) for symbol in (carry_over or [])}
    existing.discard("")
    carried = len(existing - selected)
    selected |= existing

    return sorted(selected), UniverseStats(
        listings=len(rows),
        after_security_filter=len(tradeable),
        after_market_cap_filter=above_floor,
        carried_over=carried,
        total=len(selected),
    )


def fetch_screener_rows(exchanges: tuple[str, ...] = DEFAULT_EXCHANGES) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for exchange in exchanges:
        request = urllib.request.Request(
            SCREENER_URL.format(exchange=exchange),
            headers={"User-Agent": USER_AGENT, "Accept": "application/json"},
        )
        with urllib.request.urlopen(request, timeout=60) as response:  # noqa: S310 - fixed host
            payload = json.loads(response.read().decode("utf-8"))
        table = (payload.get("data") or {}).get("table") or {}
        exchange_rows = table.get("rows") or (payload.get("data") or {}).get("rows") or []
        if not exchange_rows:
            raise RuntimeError(f"NASDAQ screener returned no rows for {exchange}")
        rows.extend(exchange_rows)
    return rows


def load_rows_from_files(paths: list[Path]) -> list[dict[str, Any]]:
    """Read screener rows from local JSON files: either a bare list or a screener response."""
    rows: list[dict[str, Any]] = []
    for path in paths:
        payload = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(payload, list):
            rows.extend(payload)
            continue
        table = (payload.get("data") or {}).get("table") or {}
        rows.extend(table.get("rows") or (payload.get("data") or {}).get("rows") or [])
    return rows


def write_universe(path: Path, symbols: list[str]) -> None:
    path.write_text("\n".join(symbols) + "\n", encoding="utf-8")


def read_existing(path: Path) -> list[str]:
    if not path.exists():
        return []
    return [
        line.strip().upper()
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip() and not line.strip().startswith("#")
    ]
