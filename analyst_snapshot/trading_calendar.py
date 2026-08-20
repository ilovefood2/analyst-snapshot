from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas_market_calendars as mcal

NEW_YORK_TZ = ZoneInfo("America/New_York")

# The scheduled job runs at 22:00 New York time, after the close of the session it archives, so the
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


def should_run_after_trading_day(
    as_of_date: date | None = None,
    force: bool = False,
    offset_days: int = DEFAULT_OFFSET_DAYS,
) -> ShouldRunReport:
    """Decide whether to snapshot, and for which trading date.

    ``as_of_date`` defaults to the current New York calendar date, which is what makes a 22:00 ET
    schedule resolve correctly even though GitHub Actions fires it on the next UTC date.
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


def print_should_run_report(
    as_of_date_raw: str | None,
    github_output: str | None,
    force: bool,
    offset_days: int = DEFAULT_OFFSET_DAYS,
) -> None:
    as_of_date = date.fromisoformat(as_of_date_raw) if as_of_date_raw else None
    report = should_run_after_trading_day(as_of_date, force, offset_days)
    print(json.dumps(asdict(report), indent=2, sort_keys=True))

    if github_output:
        with Path(github_output).open("a", encoding="utf-8") as handle:
            handle.write(f"run={str(report.run).lower()}\n")
            handle.write(f"checked_date={report.checked_date}\n")
            handle.write(f"reason={report.reason}\n")
            if report.trading_date:
                handle.write(f"trading_date={report.trading_date}\n")
