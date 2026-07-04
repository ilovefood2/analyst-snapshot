from __future__ import annotations

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
