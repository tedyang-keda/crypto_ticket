from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from typing import Optional

import httpx


logger = logging.getLogger(__name__)


@dataclass(slots=True)
class FeishuNotifier:
    webhook_url: str
    timeout_seconds: float = 8.0
    _cooldowns: dict[str, float] = field(default_factory=dict)

    async def send_text(self, text: str, *, key: Optional[str] = None, cooldown_seconds: int = 0) -> bool:
        if not self.webhook_url:
            return False
        if key and cooldown_seconds > 0:
            now = asyncio.get_running_loop().time()
            last = self._cooldowns.get(key, 0.0)
            if now - last < cooldown_seconds:
                return False
            self._cooldowns[key] = now

        payload = {"msg_type": "text", "content": {"text": text}}
        try:
            async with httpx.AsyncClient(timeout=self.timeout_seconds) as client:
                response = await client.post(self.webhook_url, json=payload)
                response.raise_for_status()
            return True
        except Exception as exc:
            logger.warning("Feishu notify failed: %s", exc)
            return False
