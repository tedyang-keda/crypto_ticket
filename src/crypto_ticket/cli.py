from __future__ import annotations

import argparse
import asyncio

from .config import load_config
from .logging import configure_logging
from .service import CryptoTicketService


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="crypto-ticket")
    subparsers = parser.add_subparsers(dest="command", required=False)
    subparsers.add_parser("run", help="start collectors and aggregators")
    subparsers.add_parser("check", help="print the loaded configuration")
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    config = load_config()
    configure_logging(config.log_level)
    if args.command == "check":
        print(config)
        return
    service = CryptoTicketService(config)
    asyncio.run(service.run())
