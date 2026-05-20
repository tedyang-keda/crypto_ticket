from __future__ import annotations

import json
import mimetypes
import threading
from functools import cached_property
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Optional
from urllib.parse import parse_qs, urlparse

from ..config import AppConfig
from ..storage.mysql import MySQLHotStore
from ..timeframes import TIMEFRAME_ORDER
from .history import load_archive_bars


class DashboardRepository:
    def __init__(self, config: AppConfig):
        self.config = config
        self.mysql: Optional[MySQLHotStore] = None
        if config.enable_mysql:
            self.mysql = MySQLHotStore(
                host=config.mysql_host,
                port=config.mysql_port,
                user=config.mysql_user,
                password=config.mysql_password,
                database=config.mysql_database,
                schema_path=str(config.schema_path) if config.schema_path else None,
            )

    def list_exchanges(self) -> list[str]:
        if self.mysql:
            exchanges = self.mysql.list_symbol_exchanges()
            if exchanges:
                return exchanges
        return [exchange.name for exchange in self.config.exchanges]

    def list_symbols(self, exchange: str, *, active_only: Optional[bool] = None) -> list[dict[str, Any]]:
        if not self.mysql:
            return []
        return self.mysql.list_symbols(exchange, active_only=active_only)

    def get_snapshot(self, exchange: str, symbol: str, timeframe: str) -> dict[str, Any]:
        symbol_row = self.mysql.get_symbol_registry(exchange, symbol) if self.mysql else None
        checkpoint = self.mysql.get_bar_checkpoint(exchange, symbol, timeframe) if self.mysql else None
        bars = load_archive_bars(self.config.archive_root, exchange, timeframe, symbol, limit=400)
        return {
            "exchange": exchange,
            "symbol": symbol,
            "timeframe": timeframe,
            "symbol_row": symbol_row,
            "checkpoint": checkpoint,
            "bar_count": len(bars),
            "first_bar": bars[0] if bars else None,
            "last_bar": bars[-1] if bars else None,
        }

    def load_bars(self, exchange: str, symbol: str, timeframe: str, *, limit: int = 400) -> list[dict[str, Any]]:
        return load_archive_bars(self.config.archive_root, exchange, timeframe, symbol, limit=limit)


class DashboardHTTPServer(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True

    def __init__(self, server_address: tuple[str, int], handler_class, config: AppConfig):
        super().__init__(server_address, handler_class)
        self.config = config
        self.repo = DashboardRepository(config)


class DashboardRequestHandler(BaseHTTPRequestHandler):
    server: DashboardHTTPServer

    def log_message(self, format: str, *args: Any) -> None:  # noqa: A003
        return

    @cached_property
    def static_dir(self) -> Path:
        return Path(__file__).resolve().parent / "static"

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        path = parsed.path.rstrip("/") or "/"
        try:
            if path == "/":
                self._serve_file(self.static_dir / "index.html", "text/html; charset=utf-8")
                return
            if path == "/app.js":
                self._serve_file(self.static_dir / "app.js", "application/javascript; charset=utf-8")
                return
            if path == "/styles.css":
                self._serve_file(self.static_dir / "styles.css", "text/css; charset=utf-8")
                return
            if path == "/api/meta":
                self._json(
                    {
                        "exchanges": self.server.repo.list_exchanges(),
                        "timeframes": list(TIMEFRAME_ORDER),
                    }
                )
                return
            if path == "/api/symbols":
                query = parse_qs(parsed.query)
                exchange = self._require_param(query, "exchange")
                active = query.get("active", ["all"])[0].lower()
                active_only: Optional[bool]
                if active in {"1", "true", "yes"}:
                    active_only = True
                elif active in {"0", "false", "no"}:
                    active_only = False
                else:
                    active_only = None
                self._json({"exchange": exchange, "symbols": self.server.repo.list_symbols(exchange, active_only=active_only)})
                return
            if path == "/api/snapshot":
                query = parse_qs(parsed.query)
                exchange = self._require_param(query, "exchange")
                symbol = self._require_param(query, "symbol")
                timeframe = self._require_param(query, "timeframe")
                self._json(self.server.repo.get_snapshot(exchange, symbol, timeframe))
                return
            if path == "/api/bars":
                query = parse_qs(parsed.query)
                exchange = self._require_param(query, "exchange")
                symbol = self._require_param(query, "symbol")
                timeframe = self._require_param(query, "timeframe")
                limit = int(query.get("limit", ["400"])[0] or 400)
                self._json(
                    {
                        "exchange": exchange,
                        "symbol": symbol,
                        "timeframe": timeframe,
                        "bars": self.server.repo.load_bars(exchange, symbol, timeframe, limit=limit),
                    }
                )
                return
            self.send_error(HTTPStatus.NOT_FOUND, "Not Found")
        except ValueError as exc:
            self._json({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)
        except Exception as exc:  # pragma: no cover - defensive
            self._json({"error": str(exc)}, status=HTTPStatus.INTERNAL_SERVER_ERROR)

    def _require_param(self, query: dict[str, list[str]], name: str) -> str:
        value = (query.get(name) or [""])[0].strip()
        if not value:
            raise ValueError(f"missing required query parameter: {name}")
        return value

    def _serve_file(self, path: Path, content_type: str) -> None:
        if not path.exists():
            self.send_error(HTTPStatus.NOT_FOUND, "Not Found")
            return
        payload = path.read_bytes()
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(payload)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(payload)

    def _json(self, payload: dict[str, Any], *, status: int = HTTPStatus.OK) -> None:
        body = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)


def run_dashboard(config: AppConfig, *, host: str = "127.0.0.1", port: int = 8088) -> None:
    server = DashboardHTTPServer((host, int(port)), DashboardRequestHandler, config)
    print(f"crypto-ticket dashboard listening on http://{host}:{int(port)}")
    try:
        server.serve_forever(poll_interval=0.5)
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()

