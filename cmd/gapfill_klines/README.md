# Frozen-audit historical repairs

This operator command never initializes schema, changes subscriptions, deletes
candles, updates existing primary keys, or triggers live-service aggregation.
It inserts only explicit missing timestamp keys from a complete frozen audit.

## Inputs and eligibility

`-report` is the full timestamp audit JSON (`series`, not just `issue_series`).
The audit watermark is fixed; open candles and new candles after it are excluded.
Both exchanges' disabled `3D` intervals are always excluded.

`-snapshot` holds `captured_at_ms`, `records` and `syncs`, exported from the
persisted official inventory/subscription tables. Use `-refresh-snapshot` to
refresh that file with read-only DB queries before planning. This does not call
the official inventory HTTP endpoint: the market service's scheduled metadata
worker must already have successfully synchronized the new tables.

Repair requires a current, successful scope observation and a symbol that is
officially present, active and selected by local subscription policy. Official
absence is not mislabeled as delisting. Unsubscribed, delisted, inactive,
unavailable and stale entries are listed separately in `skipped`.

The planner covers internal gaps, closed tail buckets, bounded empty series and
leading history when an authoritative official listing time is available.
An empty series starts at an official listing date, or at the earliest canonical
lower-period bar in the same report. Native REST periods include the official
first listed bucket (which can be a partial trading day/month). Derived periods
start at the first complete target bucket at or after the boundary. Registry
`first_seen` is never treated as listing time.
Without either boundary the empty series is explicitly skipped.

Retention is enforced: 15m/30m 30 days, 1H–12H 180 days, daily and larger at most
the latest 300 complete time buckets. Leading history before existing nonempty
series is not inferred from lower-period bars or registry `first_seen`; it
requires official listing metadata. Calendar month/quarter boundaries remain
calendar-aware.

## Plan, apply and resume

Run with the same application environment (particularly `MYSQL_DSN` and exchange
REST URLs). Choose new, distinct artifact paths, in a directory with enough space.

```sh
./gapfill_klines -report audit.json -snapshot snapshot.json -refresh-snapshot \
  -plan-only -summary plan.json

./gapfill_klines -report audit.json -snapshot snapshot.json \
  -journal repair.jsonl -summary progress.json -apply \
  -health-url http://127.0.0.1:8088/readyz -max-in-flight 8 \
  -binance-rps 1 -okx-rps 1 -max-runtime 6h -min-free-gb 5
```

Plan output includes all task timestamps, skip reasons, target kind counts and
per-exchange planned tasks/targets. Progress distinguishes inserted, already
present, expired, confirmed unresolved, unverified candidates and remaining
tasks. An unverified candidate can overlap a committed insert after verification
fails; it is not reported as a confirmed missing candle. `-max-tasks` can bound
a canary.
Without `-apply`, source fetches and validation happen but no candles are written.

Resume with the same report and journal. Completed task results are preserved;
add `-retry-unresolved` to retry tasks with still-missing keys. If a saved snapshot
has become stale, add `-refresh-snapshot`. The journal rejects another report hash,
different apply mode, concurrent writers and corrupt/partial JSON lines.

The running command rechecks persisted official/subscription metadata at least
once a minute. Newly unsubscribed/inactive/absent symbols are skipped; unavailable
metadata stops the job with missing keys retained for a later retry. Exported
snapshots default to a maximum age of 15 minutes.

## Official data and safety

Binance natively supplies 15m, 30m, 1H, 2H, 4H, 6H, 12H, 1D, 1W and 1M.
Binance 2D/5D/2W use complete official 1D inputs; 3M uses complete official 1M
inputs. OKX supplies all selected periods except 5D/2W, which use complete 1D
inputs. Derived bars are written only when every source bucket is present.

Official market-session gaps, suspensions and unavailable historical source data
remain `no_data` or `partial`; the tool never fabricates candles. Corporate-action
adjustment history needing review is left unresolved rather than mixed with raw
historical bars. Existing rows, including nonfinal conflicts, are never overwritten.

There are at most 8 concurrent repair tasks per exchange. Each exchange shares
request pacing and server-directed cooldowns across its workers. CLI caps are
Binance 8 RPS and OKX 5 RPS; the default is a conservative extra 1 RPS per exchange.
This is a separate process budget, not a cross-process global limiter: account
for the live service's traffic before increasing it.

The job stops on readiness failure, low disk space, the daily 05:10–05:30 CST
maintenance window, runtime expiry, cancellation, or 8 consecutive transient
failed tasks on either exchange. Each task is bounded and transient failures get
at most 3 attempts with backoff. Successful inserts are verified from storage.
