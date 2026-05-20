from __future__ import annotations

import argparse
import asyncio
import os

from .config import load_config
from .history_backfill import backfill_bar_history, rebuild_rollups_from_history
from .logging import configure_logging
from .service import CryptoTicketService
from .dashboard.server import run_dashboard
from .storage.mysql import MySQLHotStore


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="crypto-ticket")
    subparsers = parser.add_subparsers(dest="command", required=False)
    subparsers.add_parser("run", help="start collectors and aggregators")
    subparsers.add_parser("check", help="print the loaded configuration")
    web_parser = subparsers.add_parser("web", help="start the symbol dashboard")
    web_parser.add_argument("--host", default=os.getenv("DASHBOARD_HOST", "127.0.0.1"))
    web_parser.add_argument("--port", type=int, default=int(os.getenv("DASHBOARD_PORT", "8088")))
    backfill_parser = subparsers.add_parser("backfill-history", help="load archive bars into mysql hot history")
    backfill_parser.add_argument("--exchange")
    backfill_parser.add_argument("--timeframe")
    backfill_parser.add_argument("--symbol")
    backfill_parser.add_argument("--max-files", type=int)
    backfill_parser.add_argument("--batch-size", type=int, default=1000)
    rebuild_parser = subparsers.add_parser("rebuild-rollups", help="rebuild higher timeframe bars from stored 1m history")
    rebuild_parser.add_argument("--exchange")
    rebuild_parser.add_argument("--symbol")
    rebuild_parser.add_argument("--batch-size", type=int, default=1000)
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    config = load_config()
    configure_logging(config.log_level)
    if args.command == "check":
        print(config)
        return
    if args.command == "web":
        run_dashboard(config, host=args.host, port=args.port)
        return
    if args.command == "backfill-history":
        if not config.enable_mysql:
            raise SystemExit("ENABLE_MYSQL=true is required for backfill-history")
        mysql = MySQLHotStore(
            host=config.mysql_host,
            port=config.mysql_port,
            user=config.mysql_user,
            password=config.mysql_password,
            database=config.mysql_database,
            schema_path=str(config.schema_path) if config.schema_path else None,
        )
        mysql.ensure_schema()
        stats = backfill_bar_history(
            mysql,
            config.archive_root,
            exchange=args.exchange,
            timeframe=args.timeframe,
            symbol=args.symbol,
            max_files=args.max_files,
            batch_size=args.batch_size,
        )
        print(f"backfilled rows={stats.rows} files={stats.files} skipped={stats.skipped}")
        return
    if args.command == "rebuild-rollups":
        if not config.enable_mysql:
            raise SystemExit("ENABLE_MYSQL=true is required for rebuild-rollups")
        mysql = MySQLHotStore(
            host=config.mysql_host,
            port=config.mysql_port,
            user=config.mysql_user,
            password=config.mysql_password,
            database=config.mysql_database,
            schema_path=str(config.schema_path) if config.schema_path else None,
        )
        mysql.ensure_schema()
        stats = rebuild_rollups_from_history(
            mysql,
            exchange=args.exchange,
            symbol=args.symbol,
            batch_size=args.batch_size,
        )
        print(f"rebuilt rollup rows={stats.rows}")
        return
    service = CryptoTicketService(config)
    asyncio.run(service.run())
