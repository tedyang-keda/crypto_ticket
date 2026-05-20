from __future__ import annotations

import argparse
import asyncio
import os

from .config import load_config
from .logging import configure_logging
from .service import CryptoTicketService
from .dashboard.server import run_dashboard


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="crypto-ticket")
    subparsers = parser.add_subparsers(dest="command", required=False)
    subparsers.add_parser("run", help="start collectors and aggregators")
    subparsers.add_parser("check", help="print the loaded configuration")
    web_parser = subparsers.add_parser("web", help="start the symbol dashboard")
    web_parser.add_argument("--host", default=os.getenv("DASHBOARD_HOST", "127.0.0.1"))
    web_parser.add_argument("--port", type=int, default=int(os.getenv("DASHBOARD_PORT", "8088")))
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
    service = CryptoTicketService(config)
    asyncio.run(service.run())
