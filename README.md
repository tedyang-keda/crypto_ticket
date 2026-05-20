# crypto-ticket

Crypto tick collector for OKX and Binance.

## Layout

- WebSocket collectors ingest trade ticks.
- Redis Streams buffer the live feed.
- A rolling aggregator builds `1m` first, then rolls into higher frames.
- Final bars are archived by `exchange/timeframe/week` under `data/archive/`.
- Redis keeps current bar state and symbol universe snapshots.
- MySQL stores symbol metadata, latest finalized checkpoints, and a lean indexed `bar_history` hot table for dashboard queries; tick-level writes are disabled by default.

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
```
