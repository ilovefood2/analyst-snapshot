from __future__ import annotations

from datetime import UTC, date, datetime

import pytest

from analyst_snapshot.trading_calendar import (
    completed_session_report,
    resolve_latest_completed_nyse_session,
    session_market_close_utc,
    should_run_after_trading_day,
)


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


def test_delayed_action_after_new_york_midnight_keeps_last_completed_session() -> None:
    # The 02:00Z cron may not start until after 04:00Z. 00:01 ET on Aug 27 must still describe
    # the Aug 26 close, not create an Aug 27 pre-market partition.
    now = datetime(2026, 8, 27, 4, 1, tzinfo=UTC)

    assert resolve_latest_completed_nyse_session(now) == date(2026, 8, 26)


def test_premarket_resolves_the_previous_completed_session() -> None:
    assert resolve_latest_completed_nyse_session(datetime(2026, 8, 27, 13, 0, tzinfo=UTC)) == date(
        2026, 8, 26
    )


def test_after_close_resolves_the_current_session() -> None:
    report = completed_session_report(now=datetime(2026, 8, 27, 21, 0, tzinfo=UTC))

    assert report.run is True
    assert report.trading_date == "2026-08-27"
    assert report.session_market_close_utc == "2026-08-27T20:00:00Z"


def test_market_close_tracks_dst_and_thanksgiving_half_day() -> None:
    assert session_market_close_utc(date(2026, 8, 27)) == datetime(2026, 8, 27, 20, 0, tzinfo=UTC)
    assert session_market_close_utc(date(2026, 1, 8)) == datetime(2026, 1, 8, 21, 0, tzinfo=UTC)
    assert session_market_close_utc(date(2026, 11, 27)) == datetime(2026, 11, 27, 18, 0, tzinfo=UTC)


def test_explicit_session_is_refused_until_its_actual_close() -> None:
    report = completed_session_report(
        date(2026, 11, 27), now=datetime(2026, 11, 27, 17, 59, tzinfo=UTC)
    )

    assert report.run is False
    assert report.reason == "requested_session_has_not_closed"

    exact_close = completed_session_report(
        date(2026, 11, 27), now=datetime(2026, 11, 27, 18, 0, tzinfo=UTC)
    )
    assert exact_close.run is True


def test_explicit_holiday_is_never_a_publishable_session() -> None:
    report = completed_session_report(
        date(2026, 11, 26), now=datetime(2026, 11, 27, 22, 0, tzinfo=UTC)
    )

    assert report.run is False
    assert report.reason == "requested_date_was_not_trading_session"


def test_completed_session_resolver_rejects_naive_now() -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        resolve_latest_completed_nyse_session(datetime(2026, 8, 27, 21, 0))
