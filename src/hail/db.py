from __future__ import annotations

import json
from datetime import datetime, timezone

import aiosqlite

from hail.models import MarketDefinition, PositionState, TradeIntent


class BotDatabase:
    def __init__(self, db_path: str) -> None:
        self.db_path = db_path

    async def init(self) -> None:
        async with aiosqlite.connect(self.db_path) as db:
            await db.executescript(
                """
                PRAGMA journal_mode=WAL;
                CREATE TABLE IF NOT EXISTS markets (
                    condition_id TEXT PRIMARY KEY,
                    slug TEXT NOT NULL,
                    question TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    window_minutes INTEGER NOT NULL,
                    strike REAL NOT NULL,
                    end_time TEXT NOT NULL,
                    yes_token_id TEXT NOT NULL,
                    no_token_id TEXT NOT NULL,
                    discovered_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS market_quotes (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    condition_id TEXT NOT NULL,
                    yes_bid REAL,
                    yes_ask REAL,
                    no_bid REAL,
                    no_ask REAL,
                    fair_yes_prob REAL,
                    observed_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS intents (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    condition_id TEXT NOT NULL,
                    token_id TEXT NOT NULL,
                    side TEXT NOT NULL,
                    price REAL NOT NULL,
                    size REAL NOT NULL,
                    reason TEXT NOT NULL,
                    status TEXT NOT NULL,
                    exchange_order_id TEXT,
                    created_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS positions (
                    condition_id TEXT PRIMARY KEY,
                    qty_yes REAL NOT NULL DEFAULT 0,
                    avg_yes_price REAL NOT NULL DEFAULT 0,
                    qty_no REAL NOT NULL DEFAULT 0,
                    avg_no_price REAL NOT NULL DEFAULT 0,
                    realized_pnl REAL NOT NULL DEFAULT 0,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS pnl_snapshots (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    condition_id TEXT NOT NULL,
                    realized_pnl REAL NOT NULL,
                    unrealized_pnl REAL NOT NULL,
                    total_pnl REAL NOT NULL,
                    payload TEXT NOT NULL,
                    observed_at TEXT NOT NULL
                );
                """
            )
            await db.commit()

    async def upsert_market(self, market: MarketDefinition) -> None:
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                """
                INSERT INTO markets (
                    condition_id, slug, question, symbol, window_minutes, strike,
                    end_time, yes_token_id, no_token_id, discovered_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(condition_id) DO UPDATE SET
                    slug=excluded.slug,
                    question=excluded.question,
                    symbol=excluded.symbol,
                    window_minutes=excluded.window_minutes,
                    strike=excluded.strike,
                    end_time=excluded.end_time,
                    yes_token_id=excluded.yes_token_id,
                    no_token_id=excluded.no_token_id
                """,
                (
                    market.condition_id,
                    market.slug,
                    market.question,
                    market.symbol,
                    market.window_minutes,
                    market.strike,
                    market.end_time.isoformat(),
                    market.yes_token_id,
                    market.no_token_id,
                    datetime.now(tz=timezone.utc).isoformat(),
                ),
            )
            await db.commit()

    async def record_quote(
        self,
        condition_id: str,
        yes_bid: float | None,
        yes_ask: float | None,
        no_bid: float | None,
        no_ask: float | None,
        fair_yes_prob: float | None,
    ) -> None:
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                """
                INSERT INTO market_quotes (
                    condition_id, yes_bid, yes_ask, no_bid, no_ask, fair_yes_prob, observed_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    condition_id,
                    yes_bid,
                    yes_ask,
                    no_bid,
                    no_ask,
                    fair_yes_prob,
                    datetime.now(tz=timezone.utc).isoformat(),
                ),
            )
            await db.commit()

    async def record_intent(
        self,
        intent: TradeIntent,
        status: str,
        exchange_order_id: str | None = None,
    ) -> None:
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                """
                INSERT INTO intents (
                    condition_id, token_id, side, price, size, reason, status, exchange_order_id, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    intent.condition_id,
                    intent.token_id,
                    intent.side,
                    intent.price,
                    intent.size,
                    intent.reason,
                    status,
                    exchange_order_id,
                    datetime.now(tz=timezone.utc).isoformat(),
                ),
            )
            await db.commit()

    async def get_position(self, condition_id: str) -> PositionState:
        async with aiosqlite.connect(self.db_path) as db:
            cursor = await db.execute(
                """
                SELECT qty_yes, avg_yes_price, qty_no, avg_no_price, realized_pnl
                FROM positions WHERE condition_id = ?
                """,
                (condition_id,),
            )
            row = await cursor.fetchone()
        if row is None:
            return PositionState()
        return PositionState(
            qty_yes=float(row[0]),
            avg_yes_price=float(row[1]),
            qty_no=float(row[2]),
            avg_no_price=float(row[3]),
            realized_pnl=float(row[4]),
        )

    async def upsert_position(self, condition_id: str, pos: PositionState) -> None:
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                """
                INSERT INTO positions (
                    condition_id, qty_yes, avg_yes_price, qty_no, avg_no_price, realized_pnl, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(condition_id) DO UPDATE SET
                    qty_yes=excluded.qty_yes,
                    avg_yes_price=excluded.avg_yes_price,
                    qty_no=excluded.qty_no,
                    avg_no_price=excluded.avg_no_price,
                    realized_pnl=excluded.realized_pnl,
                    updated_at=excluded.updated_at
                """,
                (
                    condition_id,
                    pos.qty_yes,
                    pos.avg_yes_price,
                    pos.qty_no,
                    pos.avg_no_price,
                    pos.realized_pnl,
                    datetime.now(tz=timezone.utc).isoformat(),
                ),
            )
            await db.commit()

    async def record_pnl(
        self,
        condition_id: str,
        realized_pnl: float,
        unrealized_pnl: float,
        payload: dict[str, float | str],
    ) -> None:
        total = realized_pnl + unrealized_pnl
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                """
                INSERT INTO pnl_snapshots (
                    condition_id, realized_pnl, unrealized_pnl, total_pnl, payload, observed_at
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    condition_id,
                    realized_pnl,
                    unrealized_pnl,
                    total,
                    json.dumps(payload),
                    datetime.now(tz=timezone.utc).isoformat(),
                ),
            )
            await db.commit()
