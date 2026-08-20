from __future__ import annotations

from datetime import date

from analyst_snapshot.trading_calendar import should_run_after_trading_day


def test_evening_run_archives_the_session_that_just_closed() -> None:
    # Friday 2026-06-05 at 22:00 ET: the trading date is that same Friday.
    report = should_run_after_trading_day(date(2026, 6, 5))

    assert report.run is True
    assert report.checked_date == "2026-06-05"
    assert report.trading_date == "2026-06-05"


def test_evening_run_skips_a_weekend_day() -> None:
    report = should_run_after_trading_day(date(2026, 6, 6))

    assert report.run is False
    assert report.trading_date is None


def test_evening_run_skips_a_market_holiday() -> None:
    report = should_run_after_trading_day(date(2026, 7, 3))

    assert report.run is False
    assert report.checked_date == "2026-07-03"


def test_offset_supports_a_morning_after_schedule() -> None:
    report = should_run_after_trading_day(date(2026, 6, 6), offset_days=1)

    assert report.run is True
    assert report.checked_date == "2026-06-05"
    assert report.trading_date == "2026-06-05"


def test_force_runs_even_when_the_checked_date_was_not_a_trading_day() -> None:
    report = should_run_after_trading_day(date(2026, 7, 4), force=True)

    assert report.run is True
    assert report.reason == "forced"
    assert report.trading_date == "2026-07-04"
