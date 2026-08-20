from __future__ import annotations

import random
import time
from collections.abc import Iterable
from dataclasses import dataclass

import yfinance as yf

from analyst_snapshot.datasets import DatasetSpec


class SymbolDelayLimiter:
    def __init__(self, delay_seconds: float) -> None:
        if delay_seconds < 0:
            raise ValueError("delay_seconds must be zero or greater")
        self._delay_seconds = delay_seconds
        self._last_call: float | None = None

    def wait(self) -> None:
        now = time.monotonic()
        if self._last_call is not None:
            sleep_for = self._delay_seconds - (now - self._last_call)
            if sleep_for > 0:
                time.sleep(sleep_for)
        self._last_call = time.monotonic()


@dataclass(frozen=True)
class BackoffPolicy:
    base_seconds: float = 2.0
    max_seconds: float = 120.0
    long_pause_after_attempt: int = 2
    long_pause_seconds: float = 60.0

    def delay_for_attempt(self, attempt: int) -> float:
        delay = min(self.max_seconds, self.base_seconds * (2**attempt))
        if attempt >= self.long_pause_after_attempt:
            delay = max(delay, self.long_pause_seconds)
        return delay + random.uniform(0, 0.25)


class YahooAnalystFetcher:
    def __init__(
        self,
        symbol_delay_seconds: float = 0.5,
        max_retries: int = 4,
        backoff_policy: BackoffPolicy | None = None,
        max_empty_retries: int = 1,
    ) -> None:
        self._limiter = SymbolDelayLimiter(symbol_delay_seconds)
        self._max_retries = max_retries
        self._backoff_policy = backoff_policy or BackoffPolicy()
        # An empty response is usually genuine "no analyst coverage" rather than throttling, and
        # roughly 1% of the universe is uncovered. Retrying those to the full error budget costs
        # over two minutes of backoff per symbol for data that will never arrive.
        self._max_empty_retries = max(0, min(max_empty_retries, max_retries))

    def fetch_symbol(self, symbol: str, specs: Iterable[DatasetSpec]) -> dict[str, object]:
        specs_list = list(specs)
        last_error: Exception | None = None
        for attempt in range(self._max_retries + 1):
            self._limiter.wait()
            try:
                ticker = yf.Ticker(symbol)
                payloads = {spec.name: self._payload_for_spec(ticker, spec) for spec in specs_list}
                if any(_has_payload(payload) for payload in payloads.values()):
                    return payloads
                if attempt >= self._max_empty_retries:
                    return payloads
                time.sleep(self._backoff_policy.delay_for_attempt(attempt))
            except Exception as exc:  # noqa: BLE001
                last_error = exc
                if attempt >= self._max_retries or not _is_retryable_yahoo_error(exc):
                    raise
                time.sleep(self._backoff_policy.delay_for_attempt(attempt))

        if last_error is not None:
            raise last_error
        return {}

    @staticmethod
    def _payload_for_spec(ticker: yf.Ticker, spec: DatasetSpec) -> object:
        if spec.name == "estimates":
            return {attribute: getattr(ticker, attribute, None) for attribute in spec.attributes}
        return getattr(ticker, spec.attributes[0], None)


def _has_payload(payload: object) -> bool:
    if payload is None:
        return False
    if isinstance(payload, dict):
        return any(_has_payload(value) for value in payload.values()) if payload else False
    empty = getattr(payload, "empty", None)
    if isinstance(empty, bool):
        return not empty
    if isinstance(payload, list):
        return len(payload) > 0
    return True


def _is_retryable_yahoo_error(exc: Exception) -> bool:
    status = getattr(getattr(exc, "response", None), "status_code", None)
    if status in {401, 429, 999}:
        return True
    text = str(exc).lower()
    return any(token in text for token in ("401", "429", "999", "too many", "rate limit"))
