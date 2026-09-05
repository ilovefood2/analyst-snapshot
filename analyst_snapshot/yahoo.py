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


@dataclass(frozen=True)
class DatasetFetchFailure:
    """A failed request is not an empty, successfully observed dataset."""

    error_type: str
    message: str
    attempts: int


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
        pending = specs_list
        payloads: dict[str, object] = {}
        # Supported yfinance setting: HTTP/parsing errors must reach our retry logic instead of
        # becoming None. This collector is serial; restore the setting for unrelated callers.
        previous_hide = yf.config.debug.hide_exceptions
        yf.config.debug.hide_exceptions = False
        try:
            for attempt in range(self._max_retries + 1):
                self._limiter.wait()
                try:
                    ticker = yf.Ticker(symbol)
                except Exception as exc:
                    if attempt >= self._max_retries or not _is_retryable_yahoo_error(exc):
                        raise
                    time.sleep(self._backoff_policy.delay_for_attempt(attempt))
                    continue
                retry_specs = []
                for spec in pending:
                    try:
                        payloads[spec.name] = self._payload_for_spec(ticker, spec)
                    except Exception as exc:  # noqa: BLE001 - retain independent healthy datasets
                        payloads[spec.name] = DatasetFetchFailure(
                            type(exc).__name__, _failure_message(exc), attempt + 1
                        )
                        if attempt < self._max_retries and _is_retryable_yahoo_error(exc):
                            retry_specs.append(spec)
                if retry_specs:
                    pending = retry_specs
                elif (
                    attempt < self._max_empty_retries
                    and not any(
                        isinstance(value, DatasetFetchFailure) for value in payloads.values()
                    )
                    and not any(_has_payload(value) for value in payloads.values())
                ):
                    pending = specs_list
                else:
                    return payloads
                time.sleep(self._backoff_policy.delay_for_attempt(attempt))
            return payloads
        finally:
            yf.config.debug.hide_exceptions = previous_hide

    @staticmethod
    def _payload_for_spec(ticker: yf.Ticker, spec: DatasetSpec) -> object:
        if spec.is_multi_table:
            return {attribute: _attribute_value(ticker, attribute) for attribute in spec.attributes}
        return _attribute_value(ticker, spec.attributes[0])


def _attribute_value(ticker: yf.Ticker, attribute: str) -> object:
    """Read a yfinance attribute, calling it when it is a getter such as get_shares_full."""
    value = getattr(ticker, attribute)
    return value() if callable(value) else value


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


def dataset_request_estimate(specs: Iterable[DatasetSpec]) -> int:
    """Rough count of Yahoo attribute reads per symbol, for run-time budgeting."""
    return sum(len(spec.attributes) for spec in specs)


def _is_retryable_yahoo_error(exc: Exception) -> bool:
    text = (str(exc) + " " + str(getattr(getattr(exc, "response", None), "text", ""))).lower()
    if "unable to access this feature" in text:
        return False  # a permission denial is not fixed by repeated anonymous requests
    status = getattr(getattr(exc, "response", None), "status_code", None)
    if status in {401, 429, 999}:
        return True
    return any(token in text for token in ("401", "429", "999", "too many", "rate limit"))


def _failure_message(exc: Exception) -> str:
    """Record the reason without persisting request URLs, cookies or crumb values."""
    response = getattr(exc, "response", None)
    status = getattr(response, "status_code", None)
    text = (str(exc) + " " + str(getattr(response, "text", ""))).lower()
    if "invalid crumb" in text:
        return "Yahoo HTTP 401: Invalid Crumb"
    if "unable to access this feature" in text:
        return "Yahoo feature access denied"
    return f"Yahoo HTTP {status}" if status is not None else f"Yahoo {type(exc).__name__}"
