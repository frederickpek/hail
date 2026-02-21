from __future__ import annotations

import logging

import httpx

from hail.config import Settings

class TelegramNotifier:
    def __init__(self, settings: Settings) -> None:
        self._token = settings.telegram_bot_token
        self._chat_id = settings.telegram_chat_id
        self._thread_id = settings.telegram_thread_id
        self._timeout = settings.request_timeout_seconds

    @property
    def enabled(self) -> bool:
        return bool(self._token and self._chat_id)

    async def send(self, message: str) -> None:
        if not self.enabled:
            return
        url = f"https://api.telegram.org/bot{self._token}/sendMessage"
        payload = {"chat_id": self._chat_id, "text": message}
        if self._thread_id is not None:
            payload["message_thread_id"] = self._thread_id
        timeout = httpx.Timeout(self._timeout)
        try:
            async with httpx.AsyncClient(timeout=timeout) as client:
                response = await client.post(url, json=payload)
                response.raise_for_status()
        except Exception as exc:  # noqa: BLE001
            logging.warning("Telegram send failed: %s", exc)
