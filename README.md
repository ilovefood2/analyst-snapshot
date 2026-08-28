# analyst-snapshot

`analyst-snapshot` archives daily point-in-time copies of analyst data from Yahoo Finance through
`yfinance`. Yahoo serves current analyst consensus and estimates, plus a historical
upgrades/downgrades table, so this app stores what was visible on each run date with `snapshot_utc`
on every row.

The guiding rule is: never lose history, never rewrite prior dates. Daily snapshots build the
point-in-time archive needed for future ML training.

If you are consuming the archive rather than running the job, start with
[`docs/DATA_CONTRACT.md`](docs/DATA_CONTRACT.md) — it describes the layout, the schemas, and the
one rule that matters for backtests: **filter on `snapshot_utc`, never on the partition date.**

## Captured Yahoo Datasets

| Code | Archive folder | yfinance source |
| --- | --- | --- |
| `a` | `recommendations` | `Ticker.recommendations` |
| `b` | `analyst_price_targets` | `Ticker.analyst_price_targets` |
| `c` | `estimates` | `Ticker.earnings_estimate`, `Ticker.revenue_estimate`, `Ticker.eps_trend`, `Ticker.eps_revisions` |
| `d` | `upgrades_downgrades` | `Ticker.upgrades_downgrades` |
| `e` | `profile` | `Ticker.info` (curated point-in-time projection) |
| `f` | `earnings` | `Ticker.calendar`, `Ticker.earnings_dates` |
| `g` | `holders` | `Ticker.major_holders`, `Ticker.institutional_holders`, `Ticker.insider_transactions`, `Ticker.insider_purchases` |
| `h` | `shares_outstanding` | `Ticker.get_shares_full()` |

Datasets `e` to `h` exist because each captures something that is **restated or drifts** and so
cannot be reconstructed after the fact: market cap, float and share count; short interest (revised
twice a month); sector and industry classification; the forward earnings date as believed on a
given day; and institutional and insider positions. A daily `marketCap` and `sector` series is
what turns a current-only universe into a point-in-time one.

Price and volume are deliberately **not** captured: they are reconstructable from any provider at
any time, so a daily snapshot of them buys nothing.

## Free Prospective Market Context

The cloud job also captures three small, official, no-key datasets before the multi-hour Yahoo
loop. These are point-in-time research inputs for market-turn probabilities, not trading signals:

| Archive folder | Official source | What it measures | Important limitation |
| --- | --- | --- | --- |
| `cftc_tff_positioning` | CFTC Traders in Financial Futures | Dealer, asset-manager and leveraged-money positions in Nasdaq-100 and S&P 500 futures | Weekly and publication-lagged |
| `occ_account_volume` | OCC Volume Query | QQQ/SPY option volume by customer, firm and market-maker account type, call/put and exchange | No buy/sell or open/close classification |
| `finra_short_volume` | FINRA Reg SHO daily files | Consolidated NMS short, short-exempt and total volume | Short-sale flow is not participant identity |

Cboe Open-Close is deliberately not fetched: it is a paid proprietary product. The collector
never uses FMP, IBKR, Databento or another paid API and records `incremental_cash_usd=0`.

Run and verify the free capture manually:

```bash
python -m analyst_snapshot market-context --resume --run-date 2026-08-24 --symbols QQQ,SPY
python -m analyst_snapshot verify-market-context --run-date 2026-08-24
```

Normalized rows use the snapshot partition date, while `source_date` records the report/trade date
inside the official source. `snapshot_utc` remains the only authority for when the data became
observable. Exact source responses and hashes are retained under `archive/_market_context_sources/`.

Daily snapshots are written to:

```text
archive/<dataset>/date=YYYY-MM-DD/data.parquet
```

Every row gets `snapshot_utc`, `symbol`, `dataset`, `run_id`, and the raw Yahoo fields returned for
that dataset. If a symbol has no analyst coverage for a dataset, the app writes a marker row with
`no_analyst_coverage=true`; this keeps `--resume` and verification accurate for uncovered tickers.

Rating-change events from `upgrades_downgrades` are deduped into two places:

```text
archive/rating_events/date=YYYY-MM-DD/data.parquet   # events first seen on that date
archive/_index/rating_events.parquet                 # cumulative deduped index
```

The dedupe key is `(symbol, event_utc, firm, fromGrade, toGrade, action)`, and `first_seen_utc`
preserves the first time an event was seen. `event_utc` is Yahoo's `GradeDate`; earlier versions
keyed on a column Yahoo never populates, which silently discarded events that repeated a firm's
previous rating. The first successful run naturally backfills the available Yahoo rating-change
history.

The cumulative index lives under `archive/_index/` rather than inside `archive/rating_events/`
because a non-partitioned file at the root of a hive-partitioned directory is scanned as if it were
a partition, which double-counts every event.

Each run also writes `archive/_manifests/date=YYYY-MM-DD/<run_id>.json` recording row counts,
failures, and the library versions that produced the partition.

## Install

Use Python 3.12:

```bash
python3.12 -m venv .venv
. .venv/bin/activate
pip install -e .
```

For development and tests:

```bash
pip install -e ".[dev]"
```

Installing the package provides an `analyst-snapshot` command equivalent to
`python -m analyst_snapshot`. Downstream projects can depend on it directly to get
`analyst_snapshot.reader`.

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
LOG_DIR=./logs
DROPBOX_REMOTE_ROOT=/DailyStockSnapshots
```

`UNIVERSE_FILE` should contain one ticker per line. Blank lines and `#` comments are ignored.

Symbols must use **Yahoo's** spelling — class shares take a hyphen (`BRK-B`, `BF-B`), not a dot or
a slash. Yahoo answers an unknown spelling with an empty response that looks exactly like "no
analyst coverage", so a misspelled ticker silently archives empty rows forever.

Rebuild the universe from the NASDAQ screener (Nasdaq + NYSE common stock and ADRs, market cap
above a floor, warrants/rights/units/preferreds dropped, symbols normalised to Yahoo spelling):

```bash
python -m analyst_snapshot build-universe --min-market-cap 300000000
```

Symbols already in the file are always kept, even if they now fall below the floor — dropping a
symbol stops its history, and point-in-time analyst data cannot be backfilled later. Pass
`--dry-run` to preview counts, or `--replace` to override the carry-over.

The current universe is about 3,465 symbols.

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

Write a snapshot to a specific partition date:

```bash
python -m analyst_snapshot run --resume --run-date 2026-07-03
```

Yahoo throttles aggressively. The app fetches serially, waits `SYMBOL_DELAY_SECONDS` between
symbols, retries throttle-like failures with exponential backoff, pauses for 60 seconds or more
after repeated failures, isolates per-symbol failures, and retries failed symbols once at the end.
An empty response is treated as genuine "no coverage" after one retry rather than consuming the
whole error budget, because a few hundred seconds of backoff per uncovered symbol adds up.

A full run of about 3,465 symbols across all eight datasets takes roughly three to five hours
depending on Yahoo behavior. The scheduled job gives the run step a 300-minute budget inside a
350-minute job, so a run that overruns still reaches the commit step and keeps what it fetched;
re-dispatching with the same `run_date` and `--resume` finishes the remainder. Use `--datasets` to
run a smaller subset when you only need part of the picture.

Rows are buffered and each partition file is rewritten once per `--flush-every` symbols (50 by
default) instead of once per symbol. A crash loses at most that many symbols' rows, which `--resume`
refetches.

Check whether the cloud scheduler should run today:

```bash
python -m analyst_snapshot should-run
```

For a specific New York calendar date:

```bash
python -m analyst_snapshot should-run --as-of-date 2026-06-05
```

This returns `run=true` when the checked date was an NYSE trading day. The scheduled job runs after
the close and archives the session that just ended, so the checked date defaults to the New York
date the job fires on. Pass `--offset-days 1` for a morning-after schedule that archives the
previous day instead.

The GitHub workflow separately gates its two UTC cron lanes so exactly one represents 18:30 New
York time. For example, this authorizes the EDT lane on a summer trading day:

```bash
python -m analyst_snapshot schedule-gate \
  --event-name schedule \
  --event-schedule "30 22 * * 1-5"
```

`workflow_dispatch` bypasses only this cron-lane gate; the completed-session publication checks
still apply.

## Verify

Report coverage against the universe, comparison rows from the previous archived date, recent
failures, and symbols with no analyst coverage:

```bash
python -m analyst_snapshot verify
```

The report defaults to the newest partition on disk. Pick a date, gate on coverage, or save the
report:

```bash
python -m analyst_snapshot verify --run-date 2026-08-18 --fail-under 0.95 --json-out verify.json
```

`--fail-under` exits non-zero when any dataset covers less than that fraction of the universe. The
report also lists `newly_uncovered_symbols` — symbols that had coverage on the previous archived
date but report none now. That is the signature of a throttled Yahoo response being archived as if
it were real data.

Inventory the archive:

```bash
python -m analyst_snapshot info
```

## Reading the Archive

`analyst_snapshot.reader` is the supported read API. It hides the partition layout and, more
importantly, makes the point-in-time filter explicit:

```python
from analyst_snapshot.reader import latest_as_of, load_rating_events, load_snapshots

# Everything, with the partition date exposed as `trading_date`.
targets = load_snapshots("archive", "analyst_price_targets", start="2026-08-01")

# Only what was actually readable at a given instant — the safe input for a backtest.
features = latest_as_of("archive", "analyst_price_targets", as_of="2026-08-18T20:00:00Z")

# Deduped rating changes, filtered by when the rating changed.
events = load_rating_events("archive", start="2026-07-01")
```

The partition date is the trading date the snapshot describes, not when it was captured. See
[`docs/DATA_CONTRACT.md`](docs/DATA_CONTRACT.md) for the full contract, including the capture-time
regimes across the archive's history.

## Compact and Repair the Event Log

`repair-events` rebuilds `archive/_index/rating_events.parquet` from the daily
`upgrades_downgrades` partitions, which are the point-in-time source of truth and are never
modified:

```bash
python -m analyst_snapshot repair-events
```

Compaction is optional. It rewrites only the cumulative index after applying the event dedupe key:

```bash
python -m analyst_snapshot compact
```

`compact` refuses to run against an index written before the dedupe key was fixed, because
collapsing on the old key would discard real events. Run `repair-events` first. Daily snapshot
partitions are never compacted or rewritten.

## Free Cloud Schedule

The repository includes `.github/workflows/daily-snapshot.yml`, which deploys the job to GitHub
Actions.

What it does:

- Runs on weekdays at 18:30 New York time. GitHub Actions receives two UTC cron events, 22:30 for
  EDT and 23:30 for EST; a fail-closed gate uses the literal `github.event.schedule` value and the
  New York UTC offset to authorize exactly one. It does not require the delayed runner's actual
  start minute to match the cron minute. It resolves the latest weekday occurrence of that literal
  cron, so a delayed Friday runner still keeps Friday after New York midnight; that occurrence date
  must itself be an NYSE session, preventing a holiday run from republishing the prior session.
- After the schedule gate passes, the partition date is resolved from the latest NYSE
  `market_close <= actual_start_time`. Manual dispatch bypasses the cron gate but never the
  completed-session publication gate.
- An explicit manual `run_date` must be a real NYSE session whose exact close has passed. `force`
  never bypasses this publication gate.
- Scheduled runs remain resumable. Manual dispatch defaults to `fresh=true`, which uses an isolated
  archive and cannot accidentally reuse a pre-close or otherwise invalid partition.
- Commits new files under `archive/` back to the repo so the point-in-time Parquet history survives
  the ephemeral cloud runner when `fresh=false`, retrying the push with a rebase if the branch moved.
  The broad `daily_prices`, its price manifest and transient checkpoints are explicitly Git-ignored;
  their durable sealed copy is the immutable Dropbox v2 generation, so a killed runner re-fetches
  prices instead of growing Git by hundreds of thousands of rows per session.
- Captures Yahoo raw and adjusted OHLCV for the exact latest 30 XNYS sessions in serial 50-symbol
  batches, covering `universe.txt` plus the 14 fixed Trend anchors.
- Verifies price exact-target/tail coverage, adjustment parity, analyst coverage, market-context
  provenance, schema, hashes and post-close PIT timestamps before sealing a recovery bundle.
- Publishes an immutable Dropbox generation and writes `_READY.json` last. A degraded or partial
  run may still be preserved in Git for diagnosis, but it can never become a recovery source.
- Supports comma-separated `symbols` for smoke tests. Symbol-scoped runs never publish recovery
  bundles.

The workflow uses the built-in `GITHUB_TOKEN`; no Yahoo API key or cloud secret is required.

Because the repo is private, GitHub Actions usage counts against your account's included private
repo minutes. The 30-session broad price tail materially increases each Daily generation, and
`.git` grows with committed archive bytes and never shrinks. The storage approach is intentionally
simple to start, but plan to move large immutable payloads to object storage such as Cloudflare R2,
Backblaze B2, or S3-compatible storage as the archive grows, keeping this workflow as the scheduler.

## Dropbox Recovery Bundles

The GitHub Actions workflow publishes only a fully sealed completed-session generation to Dropbox.
Missing Dropbox credentials, an empty inventory, a hash mismatch or an upload conflict fails the
workflow; none of those cases writes `_READY.json`.

Create a Dropbox app in the Dropbox App Console:

- Choose `Scoped access`.
- Choose `App folder`.
- Enable the `files.content.write` permission.

Set these GitHub Actions secrets:

```text
DROPBOX_APP_KEY
DROPBOX_APP_SECRET
DROPBOX_REFRESH_TOKEN
```

Dropbox does not show a refresh token directly in the app console. Generate one with the app:

```bash
export DROPBOX_APP_KEY=your_app_key
export DROPBOX_APP_SECRET=your_app_secret
python -m analyst_snapshot dropbox-auth-url
```

Open the printed URL, approve the app, copy the authorization code, then exchange it:

```bash
python -m analyst_snapshot dropbox-exchange-code --code CODE_FROM_DROPBOX
```

Store the printed refresh token as the `DROPBOX_REFRESH_TOKEN` GitHub secret.

Seal and test a recovery upload locally:

```bash
export DROPBOX_REFRESH_TOKEN=your_refresh_token
python -m analyst_snapshot daily-prices --run-date 2026-08-18 --batch-size 50
python -m analyst_snapshot verify-daily-prices --run-date 2026-08-18 --fail-under 0.95
python -m analyst_snapshot seal-recovery-bundle --run-date 2026-08-18 \
  --generation-id manual_20260818_1
python -m analyst_snapshot upload-recovery-bundle --run-date 2026-08-18
```

The publish layout is immutable and generation-addressed:

```text
/DailyStockSnapshots/date=YYYY-MM-DD/
  _READY.json
  generations/<generation_id>/
    manifest.json
    <archive-relative inventoried files>
```

`manifest.json` uses schema `swinglab_recovery_bundle_v2`. Version 2 requires the sealed
`daily_prices` Parquet and its price manifest in addition to the analyst and market-context roles.
Every file entry records the
archive-relative path, kind, byte length and SHA-256. Parquet entries additionally bind dataset,
row count, Arrow schema hash, PIT column and minimum/maximum UTC availability time. The manifest's
`manifest_identity_sha256` hashes canonical JSON excluding only that self-hash field.

The date-root `_READY.json` uses schema `swinglab_recovery_ready_v2` and points to exactly one
generation manifest. Its `ready_identity_sha256` likewise hashes canonical JSON excluding only
itself. Consumers must validate the directory date, READY, manifest self-hash, every file hash,
schema and PIT timestamp; seeing a directory or manifest alone never means the bundle is complete.

Version 1 bundles remain historical evidence but contain no price payload. A consumer may stage
them for diagnosis, but it must not infer price capability from an old READY or bundle schema.

Publication order is:

```text
immutable data files -> immutable generation manifest -> date-root _READY.json
```

Historical Yahoo PIT data cannot be recreated after the fact. A manual historical session may
package and re-upload contemporaneously archived bytes, but must not fetch today's Yahoo values and
label them as an old session. If those original bytes are absent, recovery fails closed.

The legacy `upload-dropbox` command remains available for unsealed backup maintenance, but the
scheduled workflow does not use it and SwingLab must not treat its output as recovery-ready.

To create today's fresh post-close test generation in GitHub Actions, dispatch `Daily analyst
snapshot` with:

```text
run_date=<today's completed XNYS session>
fresh=true
symbols=<empty>
```

For an App Folder Dropbox app, these paths are relative to the app's own Dropbox folder.

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

The legacy template runs daily at 22:00 local time. It no longer matches the GitHub cloud schedule,
which runs on weekdays at 18:30 New York time; do not install both as writers for the same archive.

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
ruff format --check .
```
