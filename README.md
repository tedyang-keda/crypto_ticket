# crypto-ticket

Crypto market-data collector for OKX and Binance.

## Layout

- WebSocket collectors ingest official exchange `1m` kline/candle streams.
- Live `1m` bars are held in process memory and pushed to downstream WebSocket clients on a 1s cadence.
- Final official `1m` bars are written to MySQL.
- Higher-timeframe bars are rolled up from completed `1m` bars.
- Final `1m` bars are archived by `exchange/timeframe/week` under `data/archive/`.
- MySQL stores symbol metadata and a lean partitioned `bar_history` hot table for dashboard queries.

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
ENABLE_COLLECTOR=true \
ENABLE_MOCK_SYMBOLS=false \
MYSQL_DSN='root:root123@tcp(127.0.0.1:3306)/crypto_ticket?parseTime=true' \
DASHBOARD_DIR=./web/dist \
HTTP_ADDR=127.0.0.1:8088 \
go run ./cmd/marketd

# Backfill recent official REST klines into bar_history and clear legacy Redis kline cache.
USE_MEMORY_STORE=false \
MYSQL_DSN='root:root123@tcp(127.0.0.1:3306)/crypto_ticket?parseTime=true' \
REDIS_URL='redis://127.0.0.1:6379/0' \
go run ./cmd/backfill_klines -exchanges=binance,okx -timeframes=1m,5m,15m,30m,1H -limit=300

# Test-stage reset: clear all stored bars and all Redis kline keys first.
go run ./cmd/backfill_klines -clear-all-bar-history -clear-all-redis-kline -limit=300
```

The Go service runs official exchange kline WebSocket collectors directly.
It no longer uses Redis Streams or tick-level aggregation in `marketd`.
