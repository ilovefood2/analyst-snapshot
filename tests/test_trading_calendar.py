from __future__ import annotations

from datetime import UTC, date, datetime

import pytest

from analyst_snapshot.trading_calendar import (
    DAILY_1830_NEW_YORK_CRON,
    completed_session_report,
    resolve_latest_completed_nyse_session,
    schedule_gate_report,
    session_market_close_utc,
    should_run_after_trading_day,
)


def test_evening_run_archives_the_session_that_just_closed() -> None:
    # Friday 2026-06-05 at 18:30 ET: the trading date is that same Friday.
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
    # A severely delayed 22:30Z EDT cron may not start until after 04:00Z. 00:01 ET on Aug 27 must
    # still describe the Aug 26 close, not create an Aug 27 pre-market partition.
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


def test_summer_timezone_schedule_survives_a_runner_delay() -> None:
    now = datetime(2026, 8, 27, 23, 55, tzinfo=UTC)

    active = schedule_gate_report("schedule", DAILY_1830_NEW_YORK_CRON, now=now)

    assert active.run is True
    assert active.reason == "authorized_1830_new_york_lane"
    assert active.expected_schedule == DAILY_1830_NEW_YORK_CRON


def test_delayed_friday_cron_keeps_its_occurrence_date_after_new_york_midnight() -> None:
    report = schedule_gate_report(
        "schedule",
        DAILY_1830_NEW_YORK_CRON,
        now=datetime(2026, 8, 29, 4, 10, tzinfo=UTC),
    )

    assert report.run is True
    assert report.new_york_date == "2026-08-28"
    assert report.reason == "authorized_1830_new_york_lane"


def test_winter_timezone_schedule_tracks_dst_without_a_second_lane() -> None:
    now = datetime(2026, 1, 8, 23, 45, tzinfo=UTC)

    active = schedule_gate_report("schedule", DAILY_1830_NEW_YORK_CRON, now=now)

    assert active.run is True
    assert active.expected_schedule == DAILY_1830_NEW_YORK_CRON


@pytest.mark.parametrize(
    "now",
    [
        datetime(2026, 3, 9, 22, 45, tzinfo=UTC),
        datetime(2026, 11, 2, 23, 45, tzinfo=UTC),
    ],
)
def test_schedule_gate_tracks_dst_on_the_first_weekday_after_each_transition(
    now: datetime,
) -> None:
    report = schedule_gate_report("schedule", DAILY_1830_NEW_YORK_CRON, now=now)

    assert report.run is True
    assert report.expected_schedule == DAILY_1830_NEW_YORK_CRON


def test_scheduled_holiday_is_rejected_instead_of_reusing_the_prior_session() -> None:
    # Friday July 3, 2026 is the observed Independence Day market holiday.
    report = schedule_gate_report(
        "schedule",
        DAILY_1830_NEW_YORK_CRON,
        now=datetime(2026, 7, 3, 22, 45, tzinfo=UTC),
    )

    assert report.run is False
    assert report.reason == "new_york_date_was_not_trading_session"


def test_manual_dispatch_bypasses_the_cron_and_trading_date_gate() -> None:
    report = schedule_gate_report(
        "workflow_dispatch",
        "",
        now=datetime(2026, 7, 4, 22, 45, tzinfo=UTC),
    )

    assert report.run is True
    assert report.reason == "manual_dispatch"
    assert report.expected_schedule is None


def test_unknown_schedule_and_event_fail_closed() -> None:
    now = datetime(2026, 8, 27, 22, 45, tzinfo=UTC)

    unknown_schedule = schedule_gate_report("schedule", "0 2 * * *", now=now)
    unknown_event = schedule_gate_report("push", DAILY_1830_NEW_YORK_CRON, now=now)

    assert unknown_schedule.run is False
    assert unknown_schedule.reason == "unknown_schedule"
    assert unknown_event.run is False
    assert unknown_event.reason == "unsupported_event"
