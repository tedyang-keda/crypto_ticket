from __future__ import annotations

import asyncio
import gzip
import json
from collections import defaultdict
from pathlib import Path

from .models import BarEvent
from .timeframes import partition_year_week


class WeeklyArchiveWriter:
    def __init__(self, root_dir: str | Path):
        self.root_dir = Path(root_dir)
        self.queue: asyncio.Queue[BarEvent | None] = asyncio.Queue(maxsize=50_000)
        self._stop = asyncio.Event()

    def archive_path(self, bar: BarEvent) -> Path:
        iso_year, iso_week = partition_year_week(bar.start_ms)
        return (
            self.root_dir
            / bar.exchange.lower()
            / bar.timeframe
            / f"year={iso_year}"
            / f"week={iso_week:02d}"
            / "bars.jsonl.gz"
        )

    async def submit(self, bar: BarEvent) -> None:
        await self.queue.put(bar)

    async def close(self) -> None:
        self._stop.set()
        await self.queue.put(None)

    async def run(self) -> None:
        batch: list[BarEvent] = []
        while not self._stop.is_set():
            item = await self.queue.get()
            if item is None:
                break
            batch.append(item)
            if len(batch) >= 200:
                await asyncio.to_thread(self._flush_batch, batch)
                batch.clear()
        if batch:
            await asyncio.to_thread(self._flush_batch, batch)

    def _flush_batch(self, bars: list[BarEvent]) -> None:
        grouped: dict[Path, list[BarEvent]] = defaultdict(list)
        for bar in bars:
            grouped[self.archive_path(bar)].append(bar)
        for path, items in grouped.items():
            path.parent.mkdir(parents=True, exist_ok=True)
            with gzip.open(path, "at", encoding="utf-8") as handle:
                for bar in items:
                    handle.write(json.dumps(bar.to_dict(), ensure_ascii=False, separators=(",", ":")))
                    handle.write("\n")
