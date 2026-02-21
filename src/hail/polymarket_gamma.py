from __future__ import annotations

import json
import logging
import re
from collections import Counter
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
SLUG_SYMBOL_PATTERN = re.compile(r"\b(btc|eth|sol)\b", re.IGNORECASE)
SLUG_WINDOW_PATTERN = re.compile(r"-(5m|15m)-", re.IGNORECASE)


class GammaMarketScanner:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings

    async def scan(self) -> list[MarketDefinition]:
        now = datetime.now(tz=timezone.utc)
        end_date_min = now.strftime("%Y-%m-%dT%H:%M:%SZ")
        timeout = httpx.Timeout(self._settings.request_timeout_seconds)
        all_rows: list[dict] = []
        seen_ids: set[str] = set()
        async with httpx.AsyncClient(timeout=timeout) as client:
            for tag_slug in ["5m", "15m"]:
                minutes = int(tag_slug.replace("m", ""))
                end_date_max = (now + timedelta(minutes=minutes)).strftime("%Y-%m-%dT%H:%M:%SZ")
                params = {
                    "limit": "50",
                    "closed": "false",
                    "active": "true",
                    "tag_slug": tag_slug,
                    "archived": "false",
                    "end_date_min": end_date_min,
                    "end_date_max": end_date_max,
                    "order": "endDate",
                    "ascending": "false",
                }
                # logging.info(
                #     "gamma scan request[tag=%s]: url=%s/events params=%s",
                #     tag_slug,
                #     self._settings.gamma_api_url,
                #     params,
                # )
                try:
                    response = await client.get(f"{self._settings.gamma_api_url}/events", params=params)
                    response.raise_for_status()
                    events = response.json()
                except httpx.HTTPStatusError as exc:
                    body = exc.response.text[:300] if exc.response is not None else ""
                    logging.warning(
                        "gamma scan request[tag=%s] failed: status=%s body=%s",
                        tag_slug,
                        exc.response.status_code if exc.response is not None else "unknown",
                        body,
                    )
                    continue

                extracted_markets = 0
                for event in events:
                    markets = event.get("markets") or []
                    if not isinstance(markets, list):
                        continue
                    for row in markets:
                        key = str(row.get("conditionId") or row.get("id") or "")
                        if key and key in seen_ids:
                            continue
                        if key:
                            seen_ids.add(key)
                        all_rows.append(row)
                        extracted_markets += 1
                logging.info(
                    "gamma scan response[tag=%s]: events=%s extracted_markets=%s",
                    tag_slug,
                    len(events),
                    extracted_markets,
                )

        # logging.info("gamma scan merged rows=%s unique=%s", len(all_rows), len(seen_ids))

        candidates: list[MarketDefinition] = []
        parse_reasons: Counter[str] = Counter()
        post_parse_reasons: Counter[str] = Counter()
        parse_samples: dict[str, str] = {}
        for row in all_rows:
            parsed, reason = self._parse_market_row_with_reason(row)
            if parsed is None:
                parse_reasons[reason] += 1
                if reason not in parse_samples:
                    parse_samples[reason] = str(row.get("question") or "")[:120]
                continue
            if parsed.end_time <= now:
                post_parse_reasons["ended"] += 1
                continue
            if parsed.end_time > now + timedelta(hours=2):
                # Keep near-expiry contracts where short-horizon pricing is relevant.
                post_parse_reasons["too_far_expiry"] += 1
                continue
            candidates.append(parsed)
            if len(candidates) >= self._settings.max_open_markets:
                post_parse_reasons["capped_at_max_open_markets"] += 1
                break
        if parse_reasons.get("missing_strike", 0) > 0:
            logging.info(
                "gamma scan markets missing strike (will resolve from Binance at start time): count=%s",
                parse_reasons["missing_strike"],
            )
        # logging.info(
        #     "gamma scan filtered: accepted=%s parse_rejects=%s post_parse_rejects=%s",
        #     len(candidates),
        #     dict(parse_reasons),
        #     dict(post_parse_reasons),
        # )
        # if not candidates and parse_samples:
        #     logging.info("gamma scan examples by reject reason: %s", parse_samples)
        return candidates

    def _parse_market_row_with_reason(self, row: dict) -> tuple[MarketDefinition | None, str]:
        question = str(row.get("question") or row.get("title") or "")
        symbol = self._extract_symbol_from_structured_fields(row)
        if symbol is None or symbol not in self._settings.target_symbols:
            return None, "symbol_mismatch"

        window = self._extract_window_minutes_from_structured_fields(row)
        if window is None or window not in self._settings.target_windows_minutes:
            return None, "window_mismatch"

        strike = self._extract_strike_from_structured_fields(row)
        if strike is None:
            strike = self._extract_strike_from_text_fields(row)
        if strike is None:
            # Keep market; strike will be resolved at runtime from Binance start-time price.
            strike = 0.0

        condition_id = str(row.get("conditionId") or "")
        if not condition_id:
            return None, "missing_condition_id"

        end_time = self._parse_time(row.get("endDate"))
        if end_time is None:
            return None, "invalid_end_time"

        token_ids = self._parse_json_list(row.get("clobTokenIds"))
        outcomes = self._parse_json_list(row.get("outcomes"))
        if len(token_ids) < 2 or len(outcomes) < 2:
            return None, "missing_tokens_or_outcomes"

        yes_token = None
        no_token = None
        for token, outcome in zip(token_ids, outcomes):
            upper = str(outcome).strip().upper()
            if upper == "YES":
                yes_token = str(token)
            elif upper == "NO":
                no_token = str(token)
        if (not yes_token or not no_token) and len(token_ids) >= 2:
            # For Up/Down style markets, the first/second outcomes are still complementary.
            yes_token = str(token_ids[0])
            no_token = str(token_ids[1])
        if not yes_token or not no_token:
            return None, "missing_complementary_tokens"

        return (
            MarketDefinition(
                condition_id=condition_id,
                slug=str(row.get("slug") or ""),
                question=question,
                symbol=symbol,
                window_minutes=window,
                strike=strike,
                end_time=end_time,
                yes_token_id=yes_token,
                no_token_id=no_token,
            ),
            "ok",
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
    def _extract_symbol_from_structured_fields(row: dict) -> str | None:
        slug = str(row.get("slug") or row.get("ticker") or "")
        match = SLUG_SYMBOL_PATTERN.search(slug)
        if match is None:
            return None
        return match.group(1).upper()

    @staticmethod
    def _extract_window_minutes(question: str) -> int | None:
        for minutes, pattern in WINDOW_PATTERNS.items():
            if pattern.search(question):
                return minutes
        return None

    @staticmethod
    def _extract_window_minutes_from_structured_fields(row: dict) -> int | None:
        slug = str(row.get("slug") or row.get("ticker") or "")
        match = SLUG_WINDOW_PATTERN.search(slug)
        if match is None:
            return None
        token = match.group(1).lower()
        if token == "5m":
            return 5
        if token == "15m":
            return 15
        return None

    @staticmethod
    def _extract_strike_from_structured_fields(row: dict) -> float | None:
        # Some market types may include explicit strike-like metadata.
        for key in ("strike", "strikePrice", "startPrice", "basePrice"):
            value = row.get(key)
            if value is None:
                continue
            try:
                parsed = float(value)
                if parsed > 0:
                    return parsed
            except (TypeError, ValueError):
                continue
        return None

    @staticmethod
    def _extract_strike_from_text_fields(row: dict) -> float | None:
        for key in ("question", "title", "description", "subtitle", "ticker", "slug"):
            value = row.get(key)
            if value is None:
                continue
            parsed = GammaMarketScanner._extract_strike(str(value))
            if parsed is not None and parsed > 0:
                return parsed
        return None

    @staticmethod
    def _extract_strike(question: str) -> float | None:
        match = STRIKE_PATTERN.search(question)
        if match is None:
            return None
        return float(match.group(1).replace(",", ""))
