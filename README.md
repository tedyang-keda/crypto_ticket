# crypto-ticket

Crypto tick collector for OKX and Binance.

## Layout

- WebSocket collectors ingest trade ticks.
- Redis Streams buffer the live feed.
- A minute aggregator builds finalized `1m` bars from ticks.
- After `1m` bars are written to MySQL, a second rollup aggregator updates the latest open higher-timeframe bars.
- Final `1m` bars are archived by `exchange/timeframe/week` under `data/archive/`.
- Redis keeps current bar state and symbol universe snapshots.
- MySQL stores symbol metadata, latest checkpoints, and a lean partitioned `bar_history` hot table for dashboard queries; tick-level writes are disabled by default.

## Timeframes

Supported:

`1m 5m 15m 30m 1H 2H 4H 6H 12H 1D 2D 3D 5D 1W 2W 1M 3M`

Weekly partitioning is used for archive files to keep the hot path small on a single server.

## Run

```bash
cp .env.example .env
python -m venv .venv
source .venv/bin/activate
pip install -e .[dev]
python -m crypto_ticket run
python -m crypto_ticket web --host 127.0.0.1 --port 8088
```

Backfill existing archive bars into the MySQL hot table:

```bash
python -m crypto_ticket backfill-history --exchange binance --timeframe 1m --max-files 4
python -m crypto_ticket rebuild-rollups --exchange binance
```

## Go realtime prototype

This branch also includes a Go realtime market service and a React dashboard.

```bash
go test ./...

cd web
npm install
npm run build
cd ..

# In-memory demo mode.
USE_MEMORY_STORE=true DASHBOARD_DIR=./web/dist HTTP_ADDR=127.0.0.1:8088 go run ./cmd/marketd

# MySQL-backed mode.
USE_MEMORY_STORE=false \
ENABLE_STREAM_CONSUMER=true \
ENABLE_COLLECTOR=true \
ENABLE_MOCK_SYMBOLS=false \
MYSQL_DSN='root:root123@tcp(127.0.0.1:3306)/crypto_ticket?parseTime=true' \
DASHBOARD_DIR=./web/dist \
HTTP_ADDR=127.0.0.1:8088 \
go run ./cmd/marketd
```

The Go service can now run the exchange WebSocket collectors, write normalized
ticks into Redis Streams named `ticks:{exchange}:00`, consume those streams with
the `REDIS_CONSUMER_GROUP`, aggregate live/final bars, and serve the dashboard
from the same `marketd` process.

Demo tick ingest still works for local smoke tests:

```bash
curl -X POST http://127.0.0.1:8088/api/v1/ingest/tick \
  -H 'Content-Type: application/json' \
  -d '{"exchange":"binance","symbol":"BTCUSDT","ts_ms":1779340001000,"price":100000.12,"size":0.01,"side":"buy","trade_id":"t1"}'
```
