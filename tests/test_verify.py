from __future__ import annotations

from pathlib import Path

from analyst_snapshot.storage import append_rows, dataset_path
from analyst_snapshot.verify import coverage_report, print_coverage_report


def _universe(tmp_path: Path, symbols: list[str]) -> Path:
    path = tmp_path / "universe.txt"
    path.write_text("\n".join(symbols) + "\n", encoding="utf-8")
    return path


def _write(archive: Path, date_str: str, rows: list[dict[str, object]]) -> None:
    for dataset in ("recommendations", "analyst_price_targets", "estimates", "upgrades_downgrades"):
        append_rows(dataset_path(archive, dataset, date_str), rows, dataset)


def test_report_defaults_to_the_newest_partition_not_today(tmp_path: Path) -> None:
    # The scheduled job writes to the previous trading date, so a report anchored on "today"
    # describes an empty partition and always looks like a total failure.
    archive = tmp_path / "archive"
    _write(archive, "2026-07-03", [{"symbol": "AAPL", "snapshot_utc": "t"}])

    report = coverage_report(archive, _universe(tmp_path, ["AAPL"]), tmp_path / "logs")

    assert report["run_date"] == "2026-07-03"
    assert report["min_symbol_coverage_ratio"] == 1.0


def test_missing_symbols_lower_the_coverage_ratio(tmp_path: Path) -> None:
    archive = tmp_path / "archive"
    _write(archive, "2026-07-03", [{"symbol": "AAPL", "snapshot_utc": "t"}])

    report = coverage_report(archive, _universe(tmp_path, ["AAPL", "MSFT"]), tmp_path / "logs")

    assert report["min_symbol_coverage_ratio"] == 0.5
    assert report["datasets"]["recommendations"]["missing_symbols"] == ["MSFT"]


def test_symbols_that_lost_coverage_overnight_are_flagged(tmp_path: Path) -> None:
    # A throttled Yahoo response looks exactly like "no analyst coverage" once archived.
    archive = tmp_path / "archive"
    _write(archive, "2026-07-02", [{"symbol": "AAPL", "snapshot_utc": "t1"}])
    _write(
        archive,
        "2026-07-03",
        [{"symbol": "AAPL", "snapshot_utc": "t2", "no_analyst_coverage": True}],
    )

    report = coverage_report(archive, _universe(tmp_path, ["AAPL"]), tmp_path / "logs")

    assert report["compare_date"] == "2026-07-02"
    assert report["datasets"]["recommendations"]["newly_uncovered_symbols"] == ["AAPL"]


def test_fail_under_gate_returns_non_zero(tmp_path: Path, capsys) -> None:
    archive = tmp_path / "archive"
    _write(archive, "2026-07-03", [{"symbol": "AAPL", "snapshot_utc": "t"}])
    universe = _universe(tmp_path, ["AAPL", "MSFT"])

    assert print_coverage_report(archive, universe, tmp_path / "logs", fail_under=0.95) == 1
    assert print_coverage_report(archive, universe, tmp_path / "logs", fail_under=0.25) == 0


def test_truncated_log_lines_do_not_break_the_report(tmp_path: Path) -> None:
    logs = tmp_path / "logs"
    logs.mkdir()
    (logs / "run.jsonl").write_text(
        '{"event": "symbol_failure", "symbol": "AAPL"}\n{"event": "sym',
        encoding="utf-8",
    )
    archive = tmp_path / "archive"
    _write(archive, "2026-07-03", [{"symbol": "AAPL", "snapshot_utc": "t"}])

    report = coverage_report(archive, _universe(tmp_path, ["AAPL"]), logs)

    assert [record["symbol"] for record in report["failures"]] == ["AAPL"]
