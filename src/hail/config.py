from __future__ import annotations

from functools import lru_cache
from pathlib import Path

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    private_key: str = Field(alias="PRIVATE_KEY")
    funder_address: str | None = Field(default=None, alias="FUNDER_ADDRESS")
    poly_signature_type: int = Field(default=0, alias="POLY_SIGNATURE_TYPE")
    poly_chain_id: int = Field(default=137, alias="POLY_CHAIN_ID")
    poly_host: str = Field(default="https://clob.polymarket.com", alias="POLY_HOST")
    poly_ws_url: str = Field(default="wss://clob.polymarket.com/ws/market", alias="POLY_WS_URL")
    gamma_api_url: str = Field(default="https://gamma-api.polymarket.com", alias="GAMMA_API_URL")
    binance_ws_url: str = Field(default="wss://stream.binance.com:9443/ws", alias="BINANCE_WS_URL")

    target_symbols: list[str] = Field(default=["BTC", "ETH", "SOL"], alias="TARGET_SYMBOLS")
    target_windows_minutes: list[int] = Field(default=[5, 15], alias="TARGET_WINDOWS_MINUTES")
    default_order_size: float = Field(default=5.0, alias="DEFAULT_ORDER_SIZE")
    min_entry_edge: float = Field(default=0.035, alias="MIN_ENTRY_EDGE")
    min_exit_edge: float = Field(default=0.02, alias="MIN_EXIT_EDGE")
    min_hedge_margin: float = Field(default=0.03, alias="MIN_HEDGE_MARGIN")
    max_position_per_market: float = Field(default=15.0, alias="MAX_POSITION_PER_MARKET")
    max_open_markets: int = Field(default=20, alias="MAX_OPEN_MARKETS")
    scan_interval_seconds: int = Field(default=30, alias="SCAN_INTERVAL_SECONDS")

    telegram_bot_token: str | None = Field(default=None, alias="TELEGRAM_BOT_TOKEN")
    telegram_chat_id: str | None = Field(default=None, alias="TELEGRAM_CHAT_ID")
    telegram_report_interval_seconds: int = Field(default=300, alias="TELEGRAM_REPORT_INTERVAL_SECONDS")

    log_level: str = Field(default="INFO", alias="LOG_LEVEL")
    log_dir: str = Field(default="/logs", alias="LOG_DIR")
    log_file: str = Field(default="trader.log", alias="LOG_FILE")
    data_dir: str = Field(default="/data", alias="DATA_DIR")
    db_name: str = Field(default="hail.db", alias="DB_NAME")
    request_timeout_seconds: float = Field(default=15.0, alias="REQUEST_TIMEOUT_SECONDS")

    est_annual_vol_btc: float = Field(default=0.6, alias="EST_ANNUAL_VOL_BTC")
    est_annual_vol_eth: float = Field(default=0.75, alias="EST_ANNUAL_VOL_ETH")
    est_annual_vol_sol: float = Field(default=1.0, alias="EST_ANNUAL_VOL_SOL")

    @field_validator("target_symbols", mode="before")
    @classmethod
    def _parse_symbols(cls, value: str | list[str]) -> list[str]:
        if isinstance(value, str):
            return [item.strip().upper() for item in value.split(",") if item.strip()]
        return [item.upper() for item in value]

    @field_validator("target_windows_minutes", mode="before")
    @classmethod
    def _parse_windows(cls, value: str | list[int]) -> list[int]:
        if isinstance(value, str):
            return [int(item.strip()) for item in value.split(",") if item.strip()]
        return [int(item) for item in value]

    @property
    def db_path(self) -> Path:
        return Path(self.data_dir) / self.db_name

    @property
    def log_path(self) -> Path:
        return Path(self.log_dir) / self.log_file

    @property
    def annual_vol_by_symbol(self) -> dict[str, float]:
        return {
            "BTC": self.est_annual_vol_btc,
            "ETH": self.est_annual_vol_eth,
            "SOL": self.est_annual_vol_sol,
        }


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
