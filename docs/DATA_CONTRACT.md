# Archive data contract

This is the interface downstream research code — including swinglabv3 — should build against. The
storage layout is stable; anything not described here is best-effort and may change with Yahoo.

## Layout

```text
archive/
  recommendations/date=YYYY-MM-DD/data.parquet
  analyst_price_targets/date=YYYY-MM-DD/data.parquet
  estimates/date=YYYY-MM-DD/data.parquet
  upgrades_downgrades/date=YYYY-MM-DD/data.parquet
  rating_events/date=YYYY-MM-DD/data.parquet
  _index/rating_events.parquet
  _manifests/date=YYYY-MM-DD/run_<timestamp>.json
```

`archive/<dataset>/` is a hive-partitioned Parquet dataset and can be opened directly:

```python
import pyarrow.dataset as ds

ds.dataset("archive/recommendations", format="parquet", partitioning="hive")
```

Directories starting with `_` are **not** snapshot data. Parquet dataset readers skip them by
default (`ignore_prefixes=[".", "_"]`), which is why the cumulative index and the run manifests
live there. Never put a non-partitioned file directly under `archive/<dataset>/`: it gets scanned
as if it were a partition, and every row it holds is counted twice.

## The two timestamps, and the lookahead rule

| Column | Meaning |
| --- | --- |
| `date` (partition key) | The **trading date the snapshot describes**. Exposed by the reader as `trading_date`. |
| `snapshot_utc` | The **moment the row was read from Yahoo**. This is the only honest point-in-time column. |

They are not the same instant, and the gap has changed over the life of the archive:

| Partitions | Captured at | Gap after that day's close |
| --- | --- | --- |
| `2026-07-06` … `2026-08-18` | ~12:40 UTC the **next** morning | ~20 hours |
| later partitions | ~02:00 UTC, i.e. 22:00 ET the **same** day | ~6 hours |

**Rule: filter on `snapshot_utc`, never on the partition date.** Treating `date=D` as "known at
D's close" leaks future information — for the early partitions, it leaks an entire overnight
session including the next morning's pre-market rating changes.

`analyst_snapshot.reader` enforces this with an `as_of` argument:

```python
from analyst_snapshot.reader import latest_as_of

features = latest_as_of(
    "archive", "analyst_price_targets", as_of="2026-08-18T20:00:00Z", symbols=["AAPL", "MSFT"]
)
```

`latest_as_of` returns the newest row per symbol whose `snapshot_utc` is at or before the cutoff.
A day on which Yahoo reported no coverage does not erase the previous value; the last real
observation within `lookback_days` is returned instead.

## Columns present on every dataset

| Column | Type | Notes |
| --- | --- | --- |
| `symbol` | string | Ticker as written in `universe.txt`. |
| `snapshot_utc` | string | ISO-8601 UTC, second precision, `Z` suffix. |
| `dataset` | string | Dataset name; convenient when frames are concatenated. |
| `run_id` | string | Links the row to `_manifests/date=…/<run_id>.json`. Null for pre-0.2.0 rows. |
| `no_analyst_coverage` | bool | `true` on a placeholder row written when Yahoo returned nothing. |

`no_analyst_coverage` rows carry no data. They exist so `--resume` and coverage checks can tell
"Yahoo has nothing for this symbol" apart from "this symbol was never fetched". The reader drops
them unless you pass `drop_no_coverage=False`.

Core columns per dataset are pinned to explicit Parquet types in
`analyst_snapshot.datasets.CORE_SCHEMAS`, so a pandas or pyarrow upgrade cannot change the schema
of new partitions. Columns Yahoo adds later are still archived, with inferred types.

### `recommendations`
`period` (`0m`, `-1m`, …), `strongBuy`, `buy`, `hold`, `sell`, `strongSell`. One row per period,
so group by `["symbol", "period"]`.

### `analyst_price_targets`
`current`, `low`, `high`, `mean`, `median`. One row per symbol.

### `estimates`
`estimate_table` selects which Yahoo table the row came from: `earnings_estimate`,
`revenue_estimate`, `eps_trend`, or `eps_revisions`. Group by
`["symbol", "estimate_table", "period"]`. Columns are the union of all four tables, so most are
null on any given row.

### `upgrades_downgrades`
Yahoo's full rating-change history **as visible on that trading date**. Each daily partition
restates the entire history, so consecutive partitions are ~99% identical; the value is that
restatements, corrections and deletions are visible by diffing two dates.

Columns: `event_utc`, `event_date`, `firm`, `fromGrade`, `toGrade`, `action`,
`priceTargetAction`, `currentPriceTarget`, `priorPriceTarget`.

Partitions written before 0.2.0 store Yahoo's own spellings (`GradeDate`, `Firm`, `FromGrade`,
`ToGrade`, `Action`) *in addition to* the lower-camel ones. Those pairs were verified identical
across 1.07M archived rows, so partitions from 0.2.0 on keep only the canonical names — the
duplication cost about a third of the whole archive. `analyst_snapshot.reader` fills the canonical
columns from the legacy spellings when it reads an older partition, so a query spanning the change
returns one consistent shape. Code reading the Parquet files directly must handle both, which is
the main reason to go through the reader.

### `rating_events`
Deduped rating changes, in two forms with different semantics:

- `_index/rating_events.parquet` — the **complete** deduped set, including the history backfilled
  by the first run. Use this for event studies over the full history.
- `rating_events/date=*/` — events the pipeline saw for the **first time** on that date. This is
  the point-in-time view: it answers "when did we learn about this event?".

The dedupe key is `(symbol, event_utc, firm, fromGrade, toGrade, action)`. `first_seen_utc` records
the earliest snapshot the event appeared in.

> The daily `rating_events` partitions written before 0.2.0 are missing 6,075 events that the old
> dedupe key discarded (the key referenced a column Yahoo never populates). The cumulative index
> has been rebuilt from the `upgrades_downgrades` partitions and is complete; the daily partitions
> were deliberately not rewritten, because prior dates are never modified. If you need first-seen
> dates for events before 2026-08-19, derive them from `upgrades_downgrades` rather than from the
> `rating_events` partitions.

## Known quirks

- `event_utc` is Yahoo's `GradeDate` verbatim, timezone-naive, and occasionally **dated in the
  future**. Do not use it alone to decide what was knowable when — gate on `snapshot_utc` or
  `first_seen_utc`.
- A throttled Yahoo response can look identical to genuine "no coverage". `analyst_snapshot verify`
  reports `newly_uncovered_symbols` (covered yesterday, uncovered today) to surface this; a spike
  there means a degraded run, not a corporate event.
- `universe.txt` is the current universe, not a point-in-time one. Symbols added later are absent
  from older partitions, and delisted symbols simply stop appearing. Any study over a long window
  should reconstruct the per-date universe from the partitions themselves, not from `universe.txt`,
  or it will have survivorship bias.

## Size and growth

Partitions written before 0.2.0 are about 5.9 MB per trading day (~1.5 GB/year). From 0.2.0 on it
is about 3.8 MB per day (~0.97 GB/year), from zstd compression and from dropping the duplicated
column spellings. Either way ~85% of the volume is `upgrades_downgrades` restating the same history
every day.

Because the archive is committed to git, `.git` grows by the same amount and never shrinks. Plan to
move `archive/` to object storage (R2/B2/S3) once the repository approaches a couple of gigabytes;
the layout above is designed to be copied there unchanged.
