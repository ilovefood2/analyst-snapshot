from __future__ import annotations

import pytest

from analyst_snapshot import yahoo
from analyst_snapshot.yahoo import BackoffPolicy, SymbolDelayLimiter


def test_symbol_delay_limiter_sleeps_between_symbols(monkeypatch) -> None:
    monotonic_values = iter([0.0, 0.0, 0.1, 0.5])
    sleeps: list[float] = []

    monkeypatch.setattr(yahoo.time, "monotonic", lambda: next(monotonic_values))
    monkeypatch.setattr(yahoo.time, "sleep", sleeps.append)

    limiter = SymbolDelayLimiter(0.5)
    limiter.wait()
    limiter.wait()

    assert sleeps == [0.4]


def test_backoff_policy_adds_long_pause_after_repeated_failures(monkeypatch) -> None:
    monkeypatch.setattr(yahoo.random, "uniform", lambda _start, _end: 0)
    policy = BackoffPolicy(base_seconds=2, max_seconds=120)

    assert policy.delay_for_attempt(0) == 2
    assert policy.delay_for_attempt(1) == 4
    assert policy.delay_for_attempt(2) == 60


class _StubTicker:
    def __init__(self, symbol: str) -> None:
        self.symbol = symbol
        self.recommendations = None


def test_empty_responses_are_not_retried_to_the_full_error_budget(monkeypatch) -> None:
    # Roughly one percent of the universe genuinely has no analyst coverage. Spending the whole
    # retry budget on them costs minutes of backoff per symbol for data that never arrives.
    from analyst_snapshot.datasets import DATASETS

    sleeps: list[float] = []
    attempts: list[str] = []
    monkeypatch.setattr(yahoo.time, "sleep", sleeps.append)
    monkeypatch.setattr(
        yahoo.yf, "Ticker", lambda symbol: attempts.append(symbol) or _StubTicker(symbol)
    )

    fetcher = yahoo.YahooAnalystFetcher(symbol_delay_seconds=0, max_retries=4)
    payloads = fetcher.fetch_symbol("NOCOVER", [DATASETS["a"]])

    assert payloads == {"recommendations": None}
    assert len(attempts) == 2
    assert sum(sleeps) < 5


def test_retryable_errors_still_use_the_full_budget(monkeypatch) -> None:
    from analyst_snapshot.datasets import DATASETS

    monkeypatch.setattr(yahoo.time, "sleep", lambda _seconds: None)

    calls: list[int] = []

    def _raise(_symbol: str) -> _StubTicker:
        calls.append(1)
        raise RuntimeError("429 Too Many Requests")

    monkeypatch.setattr(yahoo.yf, "Ticker", _raise)
    fetcher = yahoo.YahooAnalystFetcher(symbol_delay_seconds=0, max_retries=4)

    with pytest.raises(RuntimeError, match="429"):
        fetcher.fetch_symbol("AAPL", [DATASETS["a"]])

    assert len(calls) == 5
