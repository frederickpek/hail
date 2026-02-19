from __future__ import annotations

import json
import re
from datetime import datetime, timedelta, timezone

import httpx

from hail.config import Settings
from hail.models import MarketDefinition

SYMBOL_PATTERNS = {
    "BTC": re.compile(r"\b(BTC|BITCOIN)\b", re.IGNORECASE),
    "ETH": re.compile(r"\b(ETH|ETHEREUM)\b", re.IGNORECASE),
    "SOL": re.compile(r"\b(SOL|SOLANA)\b", re.IGNORECASE),
}

WINDOW_PATTERNS = {
    5: re.compile(r"\b5\s*(MIN|MINS|MINUTE|MINUTES)\b", re.IGNORECASE),
    15: re.compile(r"\b15\s*(MIN|MINS|MINUTE|MINUTES)\b", re.IGNORECASE),
}

STRIKE_PATTERN = re.compile(r"\$([0-9][0-9,]*(?:\.[0-9]+)?)")


class GammaMarketScanner:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings

    async def scan(self) -> list[MarketDefinition]:
        params = {
            "closed": "false",
            "limit": str(self._settings.max_open_markets * 10),
            "offset": "0",
            "order": "volume24hr",
            "ascending": "false",
        }
        timeout = httpx.Timeout(self._settings.request_timeout_seconds)
        async with httpx.AsyncClient(timeout=timeout) as client:
            response = await client.get(f"{self._settings.gamma_api_url}/markets", params=params)
            response.raise_for_status()
            rows = response.json()

        now = datetime.now(tz=timezone.utc)
        candidates: list[MarketDefinition] = []
        for row in rows:
            parsed = self._parse_market_row(row)
            if parsed is None:
                continue
            if parsed.end_time <= now:
                continue
            if parsed.end_time > now + timedelta(hours=2):
                # Keep near-expiry contracts where short-horizon pricing is relevant.
                continue
            candidates.append(parsed)
            if len(candidates) >= self._settings.max_open_markets:
                break
        return candidates

    def _parse_market_row(self, row: dict) -> MarketDefinition | None:
        question = str(row.get("question") or "")
        symbol = self._extract_symbol(question)
        if symbol is None or symbol not in self._settings.target_symbols:
            return None

        window = self._extract_window_minutes(question)
        if window is None or window not in self._settings.target_windows_minutes:
            return None

        strike = self._extract_strike(question)
        if strike is None:
            return None

        condition_id = str(row.get("conditionId") or "")
        if not condition_id:
            return None

        end_time = self._parse_time(str(row.get("endDateIso") or row.get("endDate") or ""))
        if end_time is None:
            return None

        token_ids = self._parse_json_list(row.get("clobTokenIds"))
        outcomes = self._parse_json_list(row.get("outcomes"))
        if len(token_ids) < 2 or len(outcomes) < 2:
            return None

        yes_token = None
        no_token = None
        for token, outcome in zip(token_ids, outcomes):
            upper = str(outcome).strip().upper()
            if upper == "YES":
                yes_token = str(token)
            elif upper == "NO":
                no_token = str(token)
        if not yes_token or not no_token:
            return None

        return MarketDefinition(
            condition_id=condition_id,
            slug=str(row.get("slug") or ""),
            question=question,
            symbol=symbol,
            window_minutes=window,
            strike=strike,
            end_time=end_time,
            yes_token_id=yes_token,
            no_token_id=no_token,
        )

    @staticmethod
    def _parse_json_list(value: object) -> list[str]:
        if value is None:
            return []
        if isinstance(value, list):
            return [str(v) for v in value]
        if isinstance(value, str):
            try:
                parsed = json.loads(value)
                if isinstance(parsed, list):
                    return [str(v) for v in parsed]
            except json.JSONDecodeError:
                return []
        return []

    @staticmethod
    def _parse_time(value: str) -> datetime | None:
        if not value:
            return None
        try:
            normalized = value.replace("Z", "+00:00")
            return datetime.fromisoformat(normalized).astimezone(timezone.utc)
        except ValueError:
            return None

    @staticmethod
    def _extract_symbol(question: str) -> str | None:
        for symbol, pattern in SYMBOL_PATTERNS.items():
            if pattern.search(question):
                return symbol
        return None

    @staticmethod
    def _extract_window_minutes(question: str) -> int | None:
        for minutes, pattern in WINDOW_PATTERNS.items():
            if pattern.search(question):
                return minutes
        return None

    @staticmethod
    def _extract_strike(question: str) -> float | None:
        match = STRIKE_PATTERN.search(question)
        if match is None:
            return None
        return float(match.group(1).replace(",", ""))
