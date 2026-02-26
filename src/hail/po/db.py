from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import aiosqlite

from hail.models import MarketDefinition
from hail.po.models import MarketFillSummary, PoOrder

TERMINAL_ORDER_STATUSES = {"FILLED", "CANCELED", "CANCELLED", "REJECTED", "EXPIRED"}


def utc_now_iso() -> str:
    return datetime.now(tz=timezone.utc).isoformat()


class PoDatabase:
    def __init__(self, db_path: str) -> None:
        self.db_path = db_path

    async def init(self) -> None:
        async with aiosqlite.connect(self.db_path) as db:
            await db.executescript(
                """
                PRAGMA journal_mode=WAL;

                CREATE TABLE IF NOT EXISTS po_markets (
                    condition_id TEXT PRIMARY KEY,
                    slug TEXT NOT NULL,
                    question TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    window_minutes INTEGER NOT NULL,
                    end_time TEXT NOT NULL,
                    yes_token_id TEXT NOT NULL,
                    no_token_id TEXT NOT NULL,
                    yes_target_price REAL NOT NULL,
                    no_target_price REAL NOT NULL,
                    entered_at TEXT NOT NULL,
                    yes_filled_qty REAL NOT NULL DEFAULT 0,
                    yes_avg_fill_price REAL NOT NULL DEFAULT 0,
                    no_filled_qty REAL NOT NULL DEFAULT 0,
                    no_avg_fill_price REAL NOT NULL DEFAULT 0,
                    finalized_at TEXT
                );

                CREATE TABLE IF NOT EXISTS po_orders (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    condition_id TEXT NOT NULL,
                    token_id TEXT NOT NULL,
                    outcome TEXT NOT NULL,
                    side TEXT NOT NULL,
                    price REAL NOT NULL,
                    size REAL NOT NULL,
                    reason TEXT NOT NULL,
                    status TEXT NOT NULL,
                    exchange_order_id TEXT UNIQUE,
                    filled_size REAL NOT NULL DEFAULT 0,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS po_order_updates (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    order_id INTEGER NOT NULL,
                    exchange_order_id TEXT,
                    old_status TEXT NOT NULL,
                    new_status TEXT NOT NULL,
                    old_filled_size REAL NOT NULL,
                    new_filled_size REAL NOT NULL,
                    observed_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS po_market_results (
                    condition_id TEXT PRIMARY KEY,
                    symbol TEXT NOT NULL,
                    window_minutes INTEGER NOT NULL,
                    end_time TEXT NOT NULL,
                    resolved_side TEXT NOT NULL,
                    resolution_source TEXT NOT NULL,
                    yes_filled_qty REAL NOT NULL,
                    yes_avg_fill_price REAL NOT NULL,
                    no_filled_qty REAL NOT NULL,
                    no_avg_fill_price REAL NOT NULL,
                    cost REAL NOT NULL,
                    payout REAL NOT NULL,
                    pnl REAL NOT NULL,
                    fill_profile TEXT NOT NULL,
                    is_win INTEGER NOT NULL,
                    finalized_at TEXT NOT NULL,
                    announced_at TEXT
                );

                CREATE TABLE IF NOT EXISTS po_stats (
                    id INTEGER PRIMARY KEY CHECK (id = 1),
                    markets_entered_total INTEGER NOT NULL DEFAULT 0,
                    orders_placed_total INTEGER NOT NULL DEFAULT 0,
                    orders_filled_total INTEGER NOT NULL DEFAULT 0,
                    markets_finalized_total INTEGER NOT NULL DEFAULT 0,
                    both_filled_markets_total INTEGER NOT NULL DEFAULT 0,
                    both_filled_wins_total INTEGER NOT NULL DEFAULT 0,
                    one_filled_markets_total INTEGER NOT NULL DEFAULT 0,
                    one_filled_wins_total INTEGER NOT NULL DEFAULT 0,
                    combined_markets_total INTEGER NOT NULL DEFAULT 0,
                    combined_wins_total INTEGER NOT NULL DEFAULT 0,
                    lifetime_pnl_total REAL NOT NULL DEFAULT 0,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS po_daily_windows (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    snapshot_at TEXT NOT NULL,
                    markets_entered_total INTEGER NOT NULL,
                    orders_placed_total INTEGER NOT NULL,
                    orders_filled_total INTEGER NOT NULL,
                    both_filled_markets_total INTEGER NOT NULL,
                    both_filled_wins_total INTEGER NOT NULL,
                    one_filled_markets_total INTEGER NOT NULL,
                    one_filled_wins_total INTEGER NOT NULL,
                    combined_markets_total INTEGER NOT NULL,
                    combined_wins_total INTEGER NOT NULL,
                    lifetime_pnl_total REAL NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_po_orders_condition ON po_orders(condition_id);
                CREATE INDEX IF NOT EXISTS idx_po_orders_exchange ON po_orders(exchange_order_id);
                CREATE INDEX IF NOT EXISTS idx_po_orders_status ON po_orders(status);
                CREATE INDEX IF NOT EXISTS idx_po_markets_end_time ON po_markets(end_time);
                CREATE INDEX IF NOT EXISTS idx_po_results_announced ON po_market_results(announced_at);
                """
            )
            await db.execute(
                """
                INSERT OR IGNORE INTO po_stats (id, updated_at)
                VALUES (1, ?)
                """,
                (utc_now_iso(),),
            )
            await db.commit()

    async def list_entered_condition_ids(self) -> set[str]:
        async with aiosqlite.connect(self.db_path) as db:
            cursor = await db.execute("SELECT condition_id FROM po_markets")
            rows = await cursor.fetchall()
        return {str(row[0]) for row in rows}

    async def register_market_once(
        self,
        market: MarketDefinition,
        yes_target_price: float,
        no_target_price: float,
    ) -> bool:
        now_iso = utc_now_iso()
        async with aiosqlite.connect(self.db_path) as db:
            cursor = await db.execute(
                """
                INSERT OR IGNORE INTO po_markets (
                    condition_id, slug, question, symbol, window_minutes, end_time,
                    yes_token_id, no_token_id, yes_target_price, no_target_price, entered_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    market.condition_id,
                    market.slug,
                    market.question,
                    market.symbol,
                    market.window_minutes,
                    market.end_time.isoformat(),
                    market.yes_token_id,
                    market.no_token_id,
                    yes_target_price,
                    no_target_price,
                    now_iso,
                ),
            )
            inserted = cursor.rowcount > 0
            if inserted:
                await db.execute(
                    """
                    UPDATE po_stats
                    SET markets_entered_total = markets_entered_total + 1,
                        updated_at = ?
                    WHERE id = 1
                    """,
                    (now_iso,),
                )
            await db.commit()
        return inserted

    async def record_order_created(self, order: PoOrder) -> int:
        now_iso = utc_now_iso()
        async with aiosqlite.connect(self.db_path) as db:
            cursor = await db.execute(
                """
                INSERT INTO po_orders (
                    condition_id, token_id, outcome, side, price, size, reason, status,
                    exchange_order_id, filled_size, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, NULL, 0, ?, ?)
                """,
                (
                    order.condition_id,
                    order.token_id,
                    order.outcome,
                    order.side,
                    order.price,
                    order.size,
                    order.reason,
                    "CREATED",
                    now_iso,
                    now_iso,
                ),
            )
            await db.commit()
            return int(cursor.lastrowid)

    async def mark_order_posted(self, order_id: int, exchange_order_id: str) -> None:
        now_iso = utc_now_iso()
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                """
                UPDATE po_orders
                SET status = ?, exchange_order_id = ?, updated_at = ?
                WHERE id = ?
                """,
                ("POSTED", exchange_order_id, now_iso, order_id),
            )
            await db.execute(
                """
                UPDATE po_stats
                SET orders_placed_total = orders_placed_total + 1,
                    updated_at = ?
                WHERE id = 1
                """,
                (now_iso,),
            )
            await db.commit()

    async def mark_order_status(self, order_id: int, status: str) -> None:
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                "UPDATE po_orders SET status = ?, updated_at = ? WHERE id = ?",
                (status.upper(), utc_now_iso(), order_id),
            )
            await db.commit()

    async def list_orders_for_reconcile(self) -> list[dict[str, Any]]:
        placeholders = ",".join("?" for _ in TERMINAL_ORDER_STATUSES)
        query = f"""
            SELECT id, exchange_order_id, condition_id, filled_size, status
            FROM po_orders
            WHERE exchange_order_id IS NOT NULL
              AND UPPER(status) NOT IN ({placeholders})
            ORDER BY id ASC
        """
        async with aiosqlite.connect(self.db_path) as db:
            cursor = await db.execute(query, tuple(TERMINAL_ORDER_STATUSES))
            rows = await cursor.fetchall()
        return [
            {
                "id": int(row[0]),
                "exchange_order_id": str(row[1]),
                "condition_id": str(row[2]),
                "filled_size": float(row[3]),
                "status": str(row[4]),
            }
            for row in rows
        ]

    async def upsert_order_snapshot(
        self,
        exchange_order_id: str,
        new_status: str,
        new_filled_size: float,
    ) -> tuple[bool, str | None]:
        now_iso = utc_now_iso()
        normalized_status = new_status.upper()
        if normalized_status == "MATCHED":
            normalized_status = "FILLED"
        async with aiosqlite.connect(self.db_path) as db:
            cursor = await db.execute(
                """
                SELECT id, condition_id, status, filled_size
                FROM po_orders
                WHERE exchange_order_id = ?
                """,
                (exchange_order_id,),
            )
            row = await cursor.fetchone()
            if row is None:
                return False, None

            order_id = int(row[0])
            condition_id = str(row[1])
            old_status = str(row[2]).upper()
            old_filled_size = float(row[3])
            has_changes = old_status != normalized_status or abs(old_filled_size - new_filled_size) > 1e-9
            if not has_changes:
                return False, condition_id

            await db.execute(
                """
                UPDATE po_orders
                SET status = ?, filled_size = ?, updated_at = ?
                WHERE id = ?
                """,
                (normalized_status, max(new_filled_size, 0.0), now_iso, order_id),
            )
            await db.execute(
                """
                INSERT INTO po_order_updates (
                    order_id, exchange_order_id, old_status, new_status,
                    old_filled_size, new_filled_size, observed_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    order_id,
                    exchange_order_id,
                    old_status,
                    normalized_status,
                    old_filled_size,
                    max(new_filled_size, 0.0),
                    now_iso,
                ),
            )
            if old_filled_size <= 1e-9 and new_filled_size > 1e-9:
                await db.execute(
                    """
                    UPDATE po_stats
                    SET orders_filled_total = orders_filled_total + 1,
                        updated_at = ?
                    WHERE id = 1
                    """,
                    (now_iso,),
                )
            await db.commit()

        await self.recompute_market_fill(condition_id)
        return True, condition_id

    async def recompute_market_fill(self, condition_id: str) -> MarketFillSummary:
        async with aiosqlite.connect(self.db_path) as db:
            cursor = await db.execute(
                """
                SELECT
                    SUM(CASE WHEN outcome = 'YES' THEN filled_size ELSE 0 END) AS yes_qty,
                    SUM(CASE WHEN outcome = 'YES' THEN filled_size * price ELSE 0 END) AS yes_cost,
                    SUM(CASE WHEN outcome = 'NO' THEN filled_size ELSE 0 END) AS no_qty,
                    SUM(CASE WHEN outcome = 'NO' THEN filled_size * price ELSE 0 END) AS no_cost
                FROM po_orders
                WHERE condition_id = ?
                """,
                (condition_id,),
            )
            row = await cursor.fetchone()
            yes_qty = float(row[0] or 0.0)
            yes_cost = float(row[1] or 0.0)
            no_qty = float(row[2] or 0.0)
            no_cost = float(row[3] or 0.0)
            yes_avg = (yes_cost / yes_qty) if yes_qty > 1e-9 else 0.0
            no_avg = (no_cost / no_qty) if no_qty > 1e-9 else 0.0
            await db.execute(
                """
                UPDATE po_markets
                SET yes_filled_qty = ?, yes_avg_fill_price = ?,
                    no_filled_qty = ?, no_avg_fill_price = ?
                WHERE condition_id = ?
                """,
                (yes_qty, yes_avg, no_qty, no_avg, condition_id),
            )
            await db.commit()
        return MarketFillSummary(
            condition_id=condition_id,
            yes_filled_qty=yes_qty,
            yes_avg_price=yes_avg,
            no_filled_qty=no_qty,
            no_avg_price=no_avg,
        )

    async def list_markets_pending_resolution(self, now_iso: str) -> list[dict[str, Any]]:
        async with aiosqlite.connect(self.db_path) as db:
            cursor = await db.execute(
                """
                SELECT m.condition_id, m.symbol, m.window_minutes, m.end_time
                FROM po_markets m
                WHERE m.finalized_at IS NULL
                  AND m.end_time <= ?
                  AND EXISTS (
                    SELECT 1
                    FROM po_orders o
                    WHERE o.condition_id = m.condition_id
                      AND o.exchange_order_id IS NOT NULL
                  )
                ORDER BY m.end_time ASC
                """,
                (now_iso,),
            )
            rows = await cursor.fetchall()
        return [
            {
                "condition_id": str(row[0]),
                "symbol": str(row[1]),
                "window_minutes": int(row[2]),
                "end_time": str(row[3]),
            }
            for row in rows
        ]

    async def get_market_fill_summary(self, condition_id: str) -> MarketFillSummary:
        async with aiosqlite.connect(self.db_path) as db:
            cursor = await db.execute(
                """
                SELECT yes_filled_qty, yes_avg_fill_price, no_filled_qty, no_avg_fill_price
                FROM po_markets
                WHERE condition_id = ?
                """,
                (condition_id,),
            )
            row = await cursor.fetchone()
        if row is None:
            return MarketFillSummary(
                condition_id=condition_id,
                yes_filled_qty=0.0,
                yes_avg_price=0.0,
                no_filled_qty=0.0,
                no_avg_price=0.0,
            )
        return MarketFillSummary(
            condition_id=condition_id,
            yes_filled_qty=float(row[0] or 0.0),
            yes_avg_price=float(row[1] or 0.0),
            no_filled_qty=float(row[2] or 0.0),
            no_avg_price=float(row[3] or 0.0),
        )

    async def finalize_market_result(
        self,
        *,
        condition_id: str,
        resolved_side: str,
        resolution_source: str,
        cost: float,
        payout: float,
        pnl: float,
        fill_summary: MarketFillSummary,
    ) -> bool:
        now_iso = utc_now_iso()
        resolved_side = resolved_side.upper()
        fill_profile = fill_summary.fill_profile
        is_win = 1 if pnl > 0 else 0
        async with aiosqlite.connect(self.db_path) as db:
            cursor = await db.execute(
                """
                SELECT symbol, window_minutes, end_time
                FROM po_markets
                WHERE condition_id = ?
                """,
                (condition_id,),
            )
            market_row = await cursor.fetchone()
            if market_row is None:
                return False

            check = await db.execute(
                "SELECT 1 FROM po_market_results WHERE condition_id = ?",
                (condition_id,),
            )
            exists = await check.fetchone()
            if exists is not None:
                return False

            await db.execute(
                """
                INSERT INTO po_market_results (
                    condition_id, symbol, window_minutes, end_time, resolved_side, resolution_source,
                    yes_filled_qty, yes_avg_fill_price, no_filled_qty, no_avg_fill_price,
                    cost, payout, pnl, fill_profile, is_win, finalized_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    condition_id,
                    str(market_row[0]),
                    int(market_row[1]),
                    str(market_row[2]),
                    resolved_side,
                    resolution_source,
                    fill_summary.yes_filled_qty,
                    fill_summary.yes_avg_price,
                    fill_summary.no_filled_qty,
                    fill_summary.no_avg_price,
                    cost,
                    payout,
                    pnl,
                    fill_profile,
                    is_win,
                    now_iso,
                ),
            )
            await db.execute(
                """
                UPDATE po_markets
                SET finalized_at = ?
                WHERE condition_id = ?
                """,
                (now_iso, condition_id),
            )

            await db.execute(
                """
                UPDATE po_stats
                SET markets_finalized_total = markets_finalized_total + 1,
                    lifetime_pnl_total = lifetime_pnl_total + ?,
                    updated_at = ?
                WHERE id = 1
                """,
                (pnl, now_iso),
            )
            if fill_profile == "BOTH_FILLED":
                await db.execute(
                    """
                    UPDATE po_stats
                    SET both_filled_markets_total = both_filled_markets_total + 1,
                        both_filled_wins_total = both_filled_wins_total + ?,
                        combined_markets_total = combined_markets_total + 1,
                        combined_wins_total = combined_wins_total + ?,
                        updated_at = ?
                    WHERE id = 1
                    """,
                    (is_win, is_win, now_iso),
                )
            elif fill_profile == "ONE_FILLED":
                await db.execute(
                    """
                    UPDATE po_stats
                    SET one_filled_markets_total = one_filled_markets_total + 1,
                        one_filled_wins_total = one_filled_wins_total + ?,
                        combined_markets_total = combined_markets_total + 1,
                        combined_wins_total = combined_wins_total + ?,
                        updated_at = ?
                    WHERE id = 1
                    """,
                    (is_win, is_win, now_iso),
                )
            await db.commit()
        return True

    async def get_stats(self) -> dict[str, float]:
        async with aiosqlite.connect(self.db_path) as db:
            cursor = await db.execute(
                """
                SELECT
                    markets_entered_total, orders_placed_total, orders_filled_total,
                    markets_finalized_total, both_filled_markets_total, both_filled_wins_total,
                    one_filled_markets_total, one_filled_wins_total, combined_markets_total,
                    combined_wins_total, lifetime_pnl_total
                FROM po_stats
                WHERE id = 1
                """
            )
            row = await cursor.fetchone()
        if row is None:
            return {}
        return {
            "markets_entered_total": int(row[0]),
            "orders_placed_total": int(row[1]),
            "orders_filled_total": int(row[2]),
            "markets_finalized_total": int(row[3]),
            "both_filled_markets_total": int(row[4]),
            "both_filled_wins_total": int(row[5]),
            "one_filled_markets_total": int(row[6]),
            "one_filled_wins_total": int(row[7]),
            "combined_markets_total": int(row[8]),
            "combined_wins_total": int(row[9]),
            "lifetime_pnl_total": float(row[10]),
        }

    async def get_open_market_count(self) -> int:
        async with aiosqlite.connect(self.db_path) as db:
            cursor = await db.execute(
                """
                SELECT COUNT(*)
                FROM po_markets
                WHERE finalized_at IS NULL
                """
            )
            row = await cursor.fetchone()
        return int(row[0] if row else 0)

    async def create_daily_snapshot(self, stats: dict[str, float]) -> None:
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                """
                INSERT INTO po_daily_windows (
                    snapshot_at, markets_entered_total, orders_placed_total, orders_filled_total,
                    both_filled_markets_total, both_filled_wins_total, one_filled_markets_total,
                    one_filled_wins_total, combined_markets_total, combined_wins_total,
                    lifetime_pnl_total
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    utc_now_iso(),
                    int(stats.get("markets_entered_total", 0)),
                    int(stats.get("orders_placed_total", 0)),
                    int(stats.get("orders_filled_total", 0)),
                    int(stats.get("both_filled_markets_total", 0)),
                    int(stats.get("both_filled_wins_total", 0)),
                    int(stats.get("one_filled_markets_total", 0)),
                    int(stats.get("one_filled_wins_total", 0)),
                    int(stats.get("combined_markets_total", 0)),
                    int(stats.get("combined_wins_total", 0)),
                    float(stats.get("lifetime_pnl_total", 0.0)),
                ),
            )
            await db.commit()

    async def get_previous_daily_snapshot(self) -> dict[str, float] | None:
        async with aiosqlite.connect(self.db_path) as db:
            cursor = await db.execute(
                """
                SELECT
                    markets_entered_total, orders_placed_total, orders_filled_total,
                    both_filled_markets_total, both_filled_wins_total,
                    one_filled_markets_total, one_filled_wins_total,
                    combined_markets_total, combined_wins_total, lifetime_pnl_total
                FROM po_daily_windows
                ORDER BY id DESC
                LIMIT 1
                """
            )
            row = await cursor.fetchone()
        if row is None:
            return None
        return {
            "markets_entered_total": int(row[0]),
            "orders_placed_total": int(row[1]),
            "orders_filled_total": int(row[2]),
            "both_filled_markets_total": int(row[3]),
            "both_filled_wins_total": int(row[4]),
            "one_filled_markets_total": int(row[5]),
            "one_filled_wins_total": int(row[6]),
            "combined_markets_total": int(row[7]),
            "combined_wins_total": int(row[8]),
            "lifetime_pnl_total": float(row[9]),
        }

    async def list_unannounced_results(self, limit: int = 50) -> list[dict[str, Any]]:
        async with aiosqlite.connect(self.db_path) as db:
            cursor = await db.execute(
                """
                SELECT
                    condition_id, symbol, window_minutes, end_time, resolved_side,
                    yes_filled_qty, yes_avg_fill_price, no_filled_qty, no_avg_fill_price, pnl
                FROM po_market_results
                WHERE announced_at IS NULL
                ORDER BY finalized_at ASC
                LIMIT ?
                """,
                (limit,),
            )
            rows = await cursor.fetchall()
        return [
            {
                "condition_id": str(row[0]),
                "symbol": str(row[1]),
                "window_minutes": int(row[2]),
                "end_time": str(row[3]),
                "resolved_side": str(row[4]),
                "yes_filled_qty": float(row[5]),
                "yes_avg_fill_price": float(row[6]),
                "no_filled_qty": float(row[7]),
                "no_avg_fill_price": float(row[8]),
                "pnl": float(row[9]),
            }
            for row in rows
        ]

    async def mark_result_announced(self, condition_id: str) -> None:
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                """
                UPDATE po_market_results
                SET announced_at = ?
                WHERE condition_id = ?
                """,
                (utc_now_iso(), condition_id),
            )
            await db.commit()

    async def get_dashboard_rows(self, limit: int = 500) -> list[dict[str, Any]]:
        async with aiosqlite.connect(self.db_path) as db:
            cursor = await db.execute(
                """
                SELECT
                    m.symbol, m.window_minutes, m.end_time, m.condition_id,
                    m.yes_target_price, m.no_target_price,
                    m.yes_filled_qty, m.no_filled_qty,
                    r.resolved_side, r.pnl, r.finalized_at
                FROM po_markets m
                LEFT JOIN po_market_results r ON r.condition_id = m.condition_id
                ORDER BY m.entered_at DESC
                LIMIT ?
                """,
                (limit,),
            )
            rows = await cursor.fetchall()
        return [
            {
                "symbol": str(row[0]),
                "window_minutes": int(row[1]),
                "end_time": str(row[2]),
                "condition_id": str(row[3]),
                "yes_target_price": float(row[4]),
                "no_target_price": float(row[5]),
                "yes_filled_qty": float(row[6]),
                "no_filled_qty": float(row[7]),
                "resolved_side": None if row[8] is None else str(row[8]),
                "pnl": None if row[9] is None else float(row[9]),
                "finalized_at": None if row[10] is None else str(row[10]),
            }
            for row in rows
        ]

