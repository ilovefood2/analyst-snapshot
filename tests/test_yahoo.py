from __future__ import annotations

import pytest

from analyst_snapshot import yahoo
from analyst_snapshot.yahoo import BackoffPolicy, DatasetFetchFailure, SymbolDelayLimiter


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


def test_swallowed_401_is_exposed_retried_and_not_returned_as_empty(monkeypatch) -> None:
    from analyst_snapshot.datasets import DATASETS

    calls = []
    monkeypatch.setattr(yahoo.time, "sleep", lambda _seconds: None)
    previous = yahoo.yf.config.debug.hide_exceptions

    class Ticker:
        @property
        def recommendations(self):
            calls.append(1)
            if yahoo.yf.config.debug.hide_exceptions:
                return None  # reproduces yfinance's default exception suppression
            raise RuntimeError("HTTP Error 401: Invalid Crumb; crumb=must-not-persist")

    monkeypatch.setattr(yahoo.yf, "Ticker", lambda _symbol: Ticker())
    result = yahoo.YahooAnalystFetcher(symbol_delay_seconds=0).fetch_symbol("AAPL", [DATASETS["a"]])
    failure = result["recommendations"]
    assert isinstance(failure, DatasetFetchFailure)
    assert failure.attempts == len(calls) == 5
    assert failure.message == "Yahoo HTTP 401: Invalid Crumb"
    assert "must-not-persist" not in repr(failure)
    assert yahoo.yf.config.debug.hide_exceptions == previous


def test_getter_error_is_not_swallowed_and_healthy_dataset_is_not_refetched(monkeypatch) -> None:
    from analyst_snapshot.datasets import DATASETS

    calls = {"shares": 0, "recommendations": 0}
    monkeypatch.setattr(yahoo.time, "sleep", lambda _seconds: None)

    class Ticker:
        @property
        def recommendations(self):
            calls["recommendations"] += 1
            return [{"buy": 3}]

        def get_shares_full(self):
            calls["shares"] += 1
            if calls["shares"] == 1:
                raise RuntimeError("401 Invalid Crumb")
            return [{"shares_outstanding": 100}]

    monkeypatch.setattr(yahoo.yf, "Ticker", lambda _symbol: Ticker())
    result = yahoo.YahooAnalystFetcher(symbol_delay_seconds=0).fetch_symbol(
        "AAPL", [DATASETS["a"], DATASETS["h"]]
    )
    assert calls == {"shares": 2, "recommendations": 1}
    assert result["shares_outstanding"] == [{"shares_outstanding": 100}]


def test_feature_permission_denial_is_terminal_but_other_datasets_continue(monkeypatch) -> None:
    from analyst_snapshot.datasets import DATASETS

    calls = []
    monkeypatch.setattr(yahoo.time, "sleep", lambda _seconds: pytest.fail("unneeded retry"))

    class Ticker:
        recommendations = [{"buy": 3}]

        @property
        def analyst_price_targets(self):
            calls.append(1)
            raise RuntimeError("HTTP 401: User is unable to access this feature")

    monkeypatch.setattr(yahoo.yf, "Ticker", lambda _symbol: Ticker())
    result = yahoo.YahooAnalystFetcher(symbol_delay_seconds=0).fetch_symbol(
        "AAPL", [DATASETS["b"], DATASETS["a"]]
    )
    assert len(calls) == 1
    assert isinstance(result["analyst_price_targets"], DatasetFetchFailure)
    assert result["recommendations"] == [{"buy": 3}]
