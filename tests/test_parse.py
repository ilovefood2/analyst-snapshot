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


def test_parse_upgrades_downgrades_maps_yahoo_column_names() -> None:
    # The fixture mirrors what yfinance actually returns: a GradeDate index and capitalised
    # columns. An earlier fixture used lower-case names Yahoo never sends, which is why the
    # missing GradeDate -> event key mapping went unnoticed.
    rows = parse_dataset_payload(
        "upgrades_downgrades",
        _fixture("upgrades_downgrades.json"),
        "AAPL",
        "2026-07-04T12:00:00Z",
    )
    assert len(rows) == 2
    assert rows[0]["firm"] == "Example Bank"
    assert rows[0]["toGrade"] == "Buy"
    assert rows[0]["fromGrade"] == "Hold"
    assert rows[0]["action"] == "up"
    assert rows[0]["event_utc"] == "2026-07-01T13:12:44"
    assert rows[0]["event_date"] == "2026-07-01"


def test_event_rows_are_distinguishable_by_event_time() -> None:
    from analyst_snapshot.storage import event_key

    rows = parse_dataset_payload(
        "upgrades_downgrades",
        [
            {"GradeDate": "2026-07-01T13:12:44", "Firm": "F", "ToGrade": "Buy", "Action": "main"},
            {"GradeDate": "2026-08-01T13:12:44", "Firm": "F", "ToGrade": "Buy", "Action": "main"},
        ],
        "AAPL",
        "2026-08-02T12:00:00Z",
    )

    # Two reiterations by one firm differ only by date; they must not share a dedupe key.
    assert event_key(rows[0]) != event_key(rows[1])
