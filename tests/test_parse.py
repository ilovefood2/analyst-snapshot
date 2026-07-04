from __future__ import annotations

import json
from pathlib import Path

from analyst_snapshot.datasets import parse_dataset_payload

FIXTURES = Path(__file__).parent / "fixtures"


def _fixture(name: str) -> object:
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


def test_parse_recommendations() -> None:
    rows = parse_dataset_payload(
        "recommendations",
        _fixture("recommendations.json"),
        "AAPL",
        "2026-07-04T12:00:00Z",
    )
    assert len(rows) == 2
    assert rows[0]["symbol"] == "AAPL"
    assert rows[0]["snapshot_utc"] == "2026-07-04T12:00:00Z"
    assert rows[0]["strongBuy"] == 12


def test_parse_analyst_price_targets() -> None:
    rows = parse_dataset_payload(
        "analyst_price_targets",
        _fixture("analyst_price_targets.json"),
        "AAPL",
        "2026-07-04T12:00:00Z",
    )
    assert rows[0]["mean"] == 215.5
    assert rows[0]["median"] == 220.0


def test_parse_estimates() -> None:
    rows = parse_dataset_payload(
        "estimates",
        _fixture("estimates.json"),
        "AAPL",
        "2026-07-04T12:00:00Z",
    )
    assert {row["estimate_table"] for row in rows} == {
        "earnings_estimate",
        "revenue_estimate",
        "eps_trend",
        "eps_revisions",
    }
    assert rows[0]["avg"] == 1.52


def test_parse_upgrades_downgrades() -> None:
    rows = parse_dataset_payload(
        "upgrades_downgrades",
        _fixture("upgrades_downgrades.json"),
        "AAPL",
        "2026-07-04T12:00:00Z",
    )
    assert len(rows) == 2
    assert rows[0]["firm"] == "Example Bank"
    assert rows[0]["toGrade"] == "Buy"
