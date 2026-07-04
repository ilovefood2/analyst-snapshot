from __future__ import annotations

from datetime import date

from analyst_snapshot.trading_calendar import should_run_after_trading_day


def test_should_run_on_saturday_after_trading_friday() -> None:
    report = should_run_after_trading_day(date(2026, 6, 6))

    assert report.run is True
    assert report.checked_date == "2026-06-05"
    assert report.trading_date == "2026-06-05"


def test_should_skip_after_market_holiday() -> None:
    report = should_run_after_trading_day(date(2026, 7, 4))

    assert report.run is False
    assert report.checked_date == "2026-07-03"


def test_force_runs_even_when_previous_day_was_not_trading_day() -> None:
    report = should_run_after_trading_day(date(2026, 7, 4), force=True)

    assert report.run is True
    assert report.reason == "forced"
