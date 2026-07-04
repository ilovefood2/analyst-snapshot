# analyst-snapshot

`analyst-snapshot` archives daily point-in-time copies of analyst data from Yahoo Finance through
`yfinance`. Yahoo serves current analyst consensus and estimates, plus a historical
upgrades/downgrades table, so this app stores what was visible on each run date with `snapshot_utc`
on every row.

The guiding rule is: never lose history, never rewrite prior dates. Daily snapshots build the
point-in-time archive needed for future ML training.

## Captured Yahoo Datasets

| Code | Archive folder | yfinance source |
| --- | --- | --- |
| `a` | `recommendations` | `Ticker.recommendations` |
| `b` | `analyst_price_targets` | `Ticker.analyst_price_targets` |
| `c` | `estimates` | `Ticker.earnings_estimate`, `Ticker.revenue_estimate`, `Ticker.eps_trend`, `Ticker.eps_revisions` |
| `d` | `upgrades_downgrades` | `Ticker.upgrades_downgrades` |

Daily snapshots are written to:

```text
archive/<dataset>/date=YYYY-MM-DD/data.parquet
```

Every row gets `snapshot_utc`, `symbol`, and the raw Yahoo fields returned for that dataset. If a
symbol has no analyst coverage for a dataset, the app writes a marker row with
`no_analyst_coverage=true`; this keeps `--resume` and verification accurate for uncovered tickers.

Rating-change events from `upgrades_downgrades` are also deduped into:

```text
archive/rating_events/data.parquet
```

The event log dedupes by `(symbol, date, firm, toGrade, action)` and preserves the first time an
event was seen in `first_seen_utc`. The first successful run naturally backfills the available
Yahoo rating-change history.

## Install

Use Python 3.12:

```bash
python3.12 -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt
```

For development and tests:

```bash
pip install -r requirements-dev.txt
```

## Configure

Copy `.env.example` to `.env`:

```bash
cp .env.example .env
```

Optional settings:

```text
SNAPSHOT_DIR=./archive
UNIVERSE_FILE=./universe.txt
SYMBOL_DELAY_SECONDS=0.5
```

`UNIVERSE_FILE` should contain one ticker per line. Blank lines and `#` comments are ignored.

No API key is needed.

## Run

Full daily snapshot:

```bash
python -m analyst_snapshot run
```

Resume today without refetching symbols already snapshotted for the selected datasets:

```bash
python -m analyst_snapshot run --resume
```

Fetch selected datasets:

```bash
python -m analyst_snapshot run --datasets a,b,d
```

Fetch selected symbols:

```bash
python -m analyst_snapshot run --symbols AAPL,MSFT
```

Yahoo throttles aggressively. The app fetches serially, waits `SYMBOL_DELAY_SECONDS` between
symbols, retries throttle-like failures with exponential backoff, pauses for 60 seconds or more
after repeated failures, isolates per-symbol failures, and retries failed symbols once at the end.
A full run of about 1,330 symbols should take roughly 45 to 90 minutes depending on Yahoo behavior.

## Verify

Report today coverage versus the universe, yesterday comparison rows, recent failures, and symbols
with no analyst coverage:

```bash
python -m analyst_snapshot verify
```

The report is JSON so it can be saved, diffed, or checked by another local script.

## Compact Event Log

Compaction is optional. It rewrites only `archive/rating_events/data.parquet` after applying the
same event dedupe key:

```bash
python -m analyst_snapshot compact
```

Daily snapshot partitions are not compacted or rewritten.

## Scheduling on macOS

Edit `launchd/com.local.analyst-snapshot.plist.template` and replace:

```text
/ABSOLUTE/PATH/TO/analyst-snapshot
```

with this project folder. Then copy it into your user LaunchAgents folder:

```bash
mkdir -p ~/Library/LaunchAgents
cp launchd/com.local.analyst-snapshot.plist.template \
  ~/Library/LaunchAgents/com.local.analyst-snapshot.plist
launchctl load ~/Library/LaunchAgents/com.local.analyst-snapshot.plist
```

The template runs daily at 18:30 local time. On a Mac set to Eastern Time, that is 18:30 ET after
market close.

Check loaded jobs:

```bash
launchctl list | grep analyst-snapshot
```

After the first scheduled run:

```bash
python -m analyst_snapshot verify
```

## Tests

Tests use checked-in JSON fixtures and never call the network:

```bash
pytest
ruff check .
```
