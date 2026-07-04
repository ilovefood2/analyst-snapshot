from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas_market_calendars as mcal

NEW_YORK_TZ = ZoneInfo("America/New_York")


@dataclass(frozen=True)
class ShouldRunReport:
    run: bool
    as_of_date: str
    checked_date: str
    reason: str
    trading_date: str | None = None


def should_run_after_trading_day(
    as_of_date: date | None = None,
    force: bool = False,
) -> ShouldRunReport:
    effective_date = as_of_date or datetime.now(NEW_YORK_TZ).date()
    checked_date = effective_date - timedelta(days=1)

    if force:
        return ShouldRunReport(
            run=True,
            as_of_date=effective_date.isoformat(),
            checked_date=checked_date.isoformat(),
            reason="forced",
            trading_date=checked_date.isoformat(),
        )

    if is_nyse_trading_day(checked_date):
        return ShouldRunReport(
            run=True,
            as_of_date=effective_date.isoformat(),
            checked_date=checked_date.isoformat(),
            reason="previous_date_was_trading_day",
            trading_date=checked_date.isoformat(),
        )

    return ShouldRunReport(
        run=False,
        as_of_date=effective_date.isoformat(),
        checked_date=checked_date.isoformat(),
        reason="previous_date_was_not_trading_day",
    )


def is_nyse_trading_day(day: date) -> bool:
    calendar = mcal.get_calendar("NYSE")
    schedule = calendar.schedule(start_date=day.isoformat(), end_date=day.isoformat())
    return not schedule.empty


def print_should_run_report(
    as_of_date_raw: str | None,
    github_output: str | None,
    force: bool,
) -> None:
    as_of_date = date.fromisoformat(as_of_date_raw) if as_of_date_raw else None
    report = should_run_after_trading_day(as_of_date, force)
    payload = asdict(report)
    print(json.dumps(payload, indent=2, sort_keys=True))

    if github_output:
        output_path = Path(github_output)
        with output_path.open("a", encoding="utf-8") as handle:
            handle.write(f"run={str(report.run).lower()}\n")
            handle.write(f"checked_date={report.checked_date}\n")
            handle.write(f"reason={report.reason}\n")
            if report.trading_date:
                handle.write(f"trading_date={report.trading_date}\n")
