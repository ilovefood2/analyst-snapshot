from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas_market_calendars as mcal

NEW_YORK_TZ = ZoneInfo("America/New_York")

DAILY_1830_EDT_CRON = "30 22 * * 1-5"
DAILY_1830_EST_CRON = "30 23 * * 1-5"
DAILY_1830_CRONS = frozenset((DAILY_1830_EDT_CRON, DAILY_1830_EST_CRON))
DAILY_LOCAL_TIME = time(18, 30)

# The scheduled job runs at 18:30 New York time, after the close of the session it archives, so the
# date to check is the New York date the job fires on. A morning-after schedule would use 1.
DEFAULT_OFFSET_DAYS = 0


@dataclass(frozen=True)
class ShouldRunReport:
    run: bool
    as_of_date: str
    checked_date: str
    reason: str
    offset_days: int = DEFAULT_OFFSET_DAYS
    trading_date: str | None = None
    session_market_close_utc: str | None = None


@dataclass(frozen=True)
class ScheduleGateReport:
    run: bool
    event_name: str
    event_schedule: str
    new_york_date: str
    reason: str
    expected_schedule: str | None = None


def schedule_gate_report(
    event_name: str,
    event_schedule: str | None = None,
    *,
    now: datetime | None = None,
) -> ScheduleGateReport:
    """Authorize exactly one of the two UTC cron lanes for 18:30 New York time.

    GitHub can start a scheduled run late, but ``github.event.schedule`` remains the literal cron
    expression that triggered it. The gate therefore never compares the runner's actual hour or
    minute. For scheduled events the literal cron and observed clock resolve the most recent
    weekday occurrence at or before runner start; its New York date remains authoritative even
    when queue delay crosses midnight. A scheduled occurrence must describe an XNYS session,
    preventing a holiday trigger from republishing the prior completed session.
    """

    observed = _aware_utc(now or datetime.now(UTC))
    observed_new_york_date = observed.astimezone(NEW_YORK_TZ).date()
    normalized_schedule = (event_schedule or "").strip()

    if event_name == "workflow_dispatch":
        return ScheduleGateReport(
            run=True,
            event_name=event_name,
            event_schedule=normalized_schedule,
            new_york_date=observed_new_york_date.isoformat(),
            reason="manual_dispatch",
        )

    if event_name != "schedule":
        return ScheduleGateReport(
            run=False,
            event_name=event_name,
            event_schedule=normalized_schedule,
            new_york_date=observed_new_york_date.isoformat(),
            reason="unsupported_event",
        )

    if normalized_schedule not in DAILY_1830_CRONS:
        return ScheduleGateReport(
            run=False,
            event_name=event_name,
            event_schedule=normalized_schedule,
            new_york_date=observed_new_york_date.isoformat(),
            reason="unknown_schedule",
        )

    occurrence = _latest_weekday_cron_occurrence(observed, normalized_schedule)
    scheduled_new_york_date = occurrence.astimezone(NEW_YORK_TZ).date()
    local_slot = datetime.combine(scheduled_new_york_date, DAILY_LOCAL_TIME, tzinfo=NEW_YORK_TZ)
    utc_hour = local_slot.astimezone(UTC).hour
    expected_schedule = {
        22: DAILY_1830_EDT_CRON,
        23: DAILY_1830_EST_CRON,
    }.get(utc_hour)
    if expected_schedule is None:  # pragma: no cover - protects against an invalid timezone rule
        raise RuntimeError(f"18:30 America/New_York resolved to unexpected UTC hour {utc_hour}")

    if normalized_schedule != expected_schedule:
        return ScheduleGateReport(
            run=False,
            event_name=event_name,
            event_schedule=normalized_schedule,
            new_york_date=scheduled_new_york_date.isoformat(),
            reason="inactive_dst_lane",
            expected_schedule=expected_schedule,
        )

    if not is_nyse_trading_day(scheduled_new_york_date):
        return ScheduleGateReport(
            run=False,
            event_name=event_name,
            event_schedule=normalized_schedule,
            new_york_date=scheduled_new_york_date.isoformat(),
            reason="new_york_date_was_not_trading_session",
            expected_schedule=expected_schedule,
        )

    return ScheduleGateReport(
        run=True,
        event_name=event_name,
        event_schedule=normalized_schedule,
        new_york_date=scheduled_new_york_date.isoformat(),
        reason="authorized_1830_new_york_lane",
        expected_schedule=expected_schedule,
    )


def _latest_weekday_cron_occurrence(observed: datetime, schedule: str) -> datetime:
    hour = {
        DAILY_1830_EDT_CRON: 22,
        DAILY_1830_EST_CRON: 23,
    }[schedule]
    candidate = datetime.combine(observed.date(), time(hour, 30), tzinfo=UTC)
    if candidate > observed:
        candidate -= timedelta(days=1)
    while candidate.weekday() >= 5:
        candidate -= timedelta(days=1)
    return candidate


def should_run_after_trading_day(
    as_of_date: date | None = None,
    force: bool = False,
    offset_days: int = DEFAULT_OFFSET_DAYS,
) -> ShouldRunReport:
    """Decide whether to snapshot, and for which trading date.

    ``as_of_date`` defaults to the current New York calendar date, which makes an 18:30 ET
    schedule resolve the same-day session correctly across both UTC offsets.
    """
    effective_date = as_of_date or datetime.now(NEW_YORK_TZ).date()
    checked_date = effective_date - timedelta(days=offset_days)

    if force:
        return ShouldRunReport(
            run=True,
            as_of_date=effective_date.isoformat(),
            checked_date=checked_date.isoformat(),
            reason="forced",
            offset_days=offset_days,
            trading_date=checked_date.isoformat(),
        )

    if is_nyse_trading_day(checked_date):
        return ShouldRunReport(
            run=True,
            as_of_date=effective_date.isoformat(),
            checked_date=checked_date.isoformat(),
            reason="checked_date_was_trading_day",
            offset_days=offset_days,
            trading_date=checked_date.isoformat(),
        )

    return ShouldRunReport(
        run=False,
        as_of_date=effective_date.isoformat(),
        checked_date=checked_date.isoformat(),
        reason="checked_date_was_not_trading_day",
        offset_days=offset_days,
    )


def is_nyse_trading_day(day: date) -> bool:
    calendar = mcal.get_calendar("NYSE")
    schedule = calendar.schedule(start_date=day.isoformat(), end_date=day.isoformat())
    return not schedule.empty


def session_market_close_utc(session_date: date) -> datetime:
    """Return the authoritative XNYS close for one session as an aware UTC datetime."""

    calendar = mcal.get_calendar("NYSE")
    schedule = calendar.schedule(
        start_date=session_date.isoformat(), end_date=session_date.isoformat()
    )
    if schedule.empty:
        raise ValueError(f"{session_date.isoformat()} is not an NYSE trading session")
    close = schedule.iloc[0]["market_close"].to_pydatetime()
    if close.tzinfo is None:  # pragma: no cover - pandas-market-calendars supplies aware values
        raise ValueError("NYSE market close is unexpectedly timezone-naive")
    return close.astimezone(UTC)


def resolve_latest_completed_nyse_session(now: datetime | None = None) -> date:
    """Resolve by actual market close, so a delayed Action cannot cross-label the next day."""

    observed = _aware_utc(now or datetime.now(UTC))
    new_york_date = observed.astimezone(NEW_YORK_TZ).date()
    calendar = mcal.get_calendar("NYSE")
    schedule = calendar.schedule(
        start_date=(new_york_date - timedelta(days=14)).isoformat(),
        end_date=new_york_date.isoformat(),
    )
    eligible = schedule[schedule["market_close"] <= observed]
    if eligible.empty:
        raise ValueError("no completed NYSE session found in the prior 14 calendar days")
    return eligible.index[-1].date()


def completed_session_report(
    session_date: date | None = None,
    *,
    now: datetime | None = None,
) -> ShouldRunReport:
    """Validate an explicit session or resolve the latest session whose close has passed."""

    observed = _aware_utc(now or datetime.now(UTC))
    if session_date is None:
        target = resolve_latest_completed_nyse_session(observed)
        close = session_market_close_utc(target)
        reason = "latest_completed_nyse_session"
    else:
        target = session_date
        try:
            close = session_market_close_utc(target)
        except ValueError:
            return ShouldRunReport(
                run=False,
                as_of_date=observed.astimezone(NEW_YORK_TZ).date().isoformat(),
                checked_date=target.isoformat(),
                reason="requested_date_was_not_trading_session",
            )
        if close > observed:
            return ShouldRunReport(
                run=False,
                as_of_date=observed.astimezone(NEW_YORK_TZ).date().isoformat(),
                checked_date=target.isoformat(),
                reason="requested_session_has_not_closed",
                trading_date=target.isoformat(),
                session_market_close_utc=_iso_utc(close),
            )
        reason = "requested_completed_nyse_session"
    return ShouldRunReport(
        run=True,
        as_of_date=observed.astimezone(NEW_YORK_TZ).date().isoformat(),
        checked_date=target.isoformat(),
        reason=reason,
        trading_date=target.isoformat(),
        session_market_close_utc=_iso_utc(close),
    )


def _aware_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        raise ValueError("now must be timezone-aware")
    return value.astimezone(UTC)


def _iso_utc(value: datetime) -> str:
    return value.astimezone(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def print_schedule_gate_report(
    event_name: str,
    event_schedule: str | None,
    github_output: str | None,
    now_utc_raw: str | None = None,
) -> None:
    now = datetime.fromisoformat(now_utc_raw.replace("Z", "+00:00")) if now_utc_raw else None
    report = schedule_gate_report(event_name, event_schedule, now=now)
    print(json.dumps(asdict(report), indent=2, sort_keys=True))

    if github_output:
        with Path(github_output).open("a", encoding="utf-8") as handle:
            handle.write(f"run={str(report.run).lower()}\n")
            handle.write(f"reason={report.reason}\n")
            handle.write(f"new_york_date={report.new_york_date}\n")
            if report.expected_schedule:
                handle.write(f"expected_schedule={report.expected_schedule}\n")


def print_should_run_report(
    as_of_date_raw: str | None,
    github_output: str | None,
    force: bool,
    offset_days: int = DEFAULT_OFFSET_DAYS,
    session_date_raw: str | None = None,
    now_utc_raw: str | None = None,
) -> None:
    if session_date_raw and as_of_date_raw:
        raise ValueError("--session-date and --as-of-date are mutually exclusive")
    now = datetime.fromisoformat(now_utc_raw.replace("Z", "+00:00")) if now_utc_raw else None
    if session_date_raw:
        # Publishing authority is never forced for a non-session or a session that has not closed.
        report = completed_session_report(date.fromisoformat(session_date_raw), now=now)
    elif as_of_date_raw:
        report = should_run_after_trading_day(
            date.fromisoformat(as_of_date_raw), force, offset_days
        )
    else:
        report = completed_session_report(now=now)
    print(json.dumps(asdict(report), indent=2, sort_keys=True))

    if github_output:
        with Path(github_output).open("a", encoding="utf-8") as handle:
            handle.write(f"run={str(report.run).lower()}\n")
            handle.write(f"checked_date={report.checked_date}\n")
            handle.write(f"reason={report.reason}\n")
            if report.trading_date:
                handle.write(f"trading_date={report.trading_date}\n")
            if report.session_market_close_utc:
                handle.write(f"session_market_close_utc={report.session_market_close_utc}\n")
