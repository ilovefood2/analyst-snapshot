from __future__ import annotations

from pathlib import Path

from analyst_snapshot.universe import (
    is_tradeable_security,
    market_cap,
    normalize_symbol,
    read_existing,
    select_symbols,
    write_universe,
)


def test_class_shares_are_normalised_to_yahoos_spelling() -> None:
    # Yahoo answers only to BRK-B. Asking for BRK.B or BRK/B returns an empty response that is
    # indistinguishable from "no analyst coverage" — which is what silently happened for 31 days.
    assert normalize_symbol("BRK/B") == "BRK-B"
    assert normalize_symbol("BRK.B") == "BRK-B"
    assert normalize_symbol(" brk-b ") == "BRK-B"
    assert normalize_symbol("AAPL") == "AAPL"


def test_bare_corporate_names_are_kept() -> None:
    # Most common stocks carry no security-type suffix, so a whitelist of name patterns drops
    # household names.
    for name in ("Visa Inc.", "Berkshire Hathaway Inc.", "AT&T Inc."):
        assert is_tradeable_security(name)


def test_adrs_are_kept_but_preferred_depositary_shares_are_not() -> None:
    assert is_tradeable_security("Shell PLC American Depositary Shares (each representing two)")
    assert not is_tradeable_security(
        "Acme Inc. Depositary Shares each representing 1/1000th of 6.50% Series A Preferred Stock"
    )


def test_non_equity_securities_are_dropped() -> None:
    for name in (
        "Artius II Acquisition Inc. Warrants",
        "Artius II Acquisition Inc. Rights",
        "Artius II Acquisition Inc. Units",
        "Acme Capital 7.25% Notes due 2054",
    ):
        assert not is_tradeable_security(name)


def test_market_cap_parsing_handles_screener_formatting() -> None:
    assert market_cap({"marketCap": "1,234,000.00"}) == 1_234_000.0
    assert market_cap({"marketCap": "0.00"}) == 0.0
    assert market_cap({"marketCap": None}) == 0.0
    assert market_cap({"marketCap": "n/a"}) == 0.0


def _rows() -> list[dict[str, object]]:
    return [
        {"symbol": "AAPL", "name": "Apple Inc. Common Stock", "marketCap": "3200000000000.00"},
        {"symbol": "BRK/B", "name": "Berkshire Hathaway Inc.", "marketCap": "1100000000000.00"},
        {"symbol": "TINY", "name": "Tiny Corp Common Stock", "marketCap": "50000000.00"},
        {"symbol": "ACME^A", "name": "Acme Inc. Preferred", "marketCap": "9000000000.00"},
        {"symbol": "SPAKW", "name": "Spac Acquisition Warrants", "marketCap": "9000000000.00"},
    ]


def test_selection_applies_the_floor_and_drops_non_equity() -> None:
    symbols, stats = select_symbols(_rows(), min_market_cap=300_000_000.0)

    assert symbols == ["AAPL", "BRK-B"]
    assert stats.after_market_cap_filter == 2
    assert stats.carried_over == 0


def test_existing_symbols_are_never_dropped() -> None:
    # A symbol removed from the universe stops accruing history, and point-in-time analyst data
    # cannot be backfilled later.
    symbols, stats = select_symbols(
        _rows(), min_market_cap=300_000_000.0, carry_over=["TINY", "BRK.B", "GONE"]
    )

    assert symbols == ["AAPL", "BRK-B", "GONE", "TINY"]
    assert stats.carried_over == 2


def test_round_trip_through_the_universe_file(tmp_path: Path) -> None:
    path = tmp_path / "universe.txt"
    write_universe(path, ["AAPL", "BRK-B"])

    assert read_existing(path) == ["AAPL", "BRK-B"]
