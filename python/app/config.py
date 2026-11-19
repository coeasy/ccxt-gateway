from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from typing import Any


def _positive_int(env: dict[str, str], name: str, default: int) -> int:
    raw = env.get(name)
    if raw in (None, ""):
        return default
    try:
        value = int(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be a positive integer") from exc
    if value <= 0:
        raise ValueError(f"{name} must be a positive integer")
    return value


@dataclass(frozen=True, slots=True)
class GatewayConfig:
    api_key: str
    host: str = "0.0.0.0"
    port: int = 8080
    exchange_credentials: dict[str, dict[str, Any]] = field(default_factory=dict)
    exchange_idle_ttl: float = 60.0
    exchange_max_instances: int = 32
    exchange_timeout_ms: int = 30_000
    ws_idle_grace: float = 10.0
    ws_max_clients: int = 500
    ws_max_streams: int = 1_000
    cache_max_entries: int = 10_000
    cache_ticker_ttl: float = 2.0
    cache_orderbook_ttl: float = 1.0
    cache_trades_ttl: float = 1.0
    cache_ohlcv_ttl: float = 5.0
    cache_markets_ttl: float = 300.0
    rate_limit_requests: int = 120
    rate_limit_window: float = 60.0

    @classmethod
    def from_env(cls, env: dict[str, str] | None = None) -> GatewayConfig:
        source = dict(os.environ if env is None else env)
        key = source.get("API_KEY", "").strip()
        if len(key) < 32:
            raise ValueError("API_KEY is required and must be at least 32 characters")
        raw_credentials = source.get("EXCHANGE_CREDENTIALS_JSON", "{}")
        try:
            credentials = json.loads(raw_credentials)
        except json.JSONDecodeError as exc:
            raise ValueError("EXCHANGE_CREDENTIALS_JSON must contain valid JSON") from exc
        if not isinstance(credentials, dict):
            raise ValueError("EXCHANGE_CREDENTIALS_JSON must be a JSON object")
        for exchange_id, value in credentials.items():
            if (
                not exchange_id.isascii()
                or not exchange_id.isalnum()
                or not isinstance(value, dict)
            ):
                raise ValueError(f"Invalid credentials entry for exchange {exchange_id!r}")
            for field_name in ("apiKey", "secret", "password", "uid", "token"):
                if field_name in value and not isinstance(value[field_name], str):
                    raise ValueError(f"{exchange_id}.{field_name} must be a string")

        port = _positive_int(source, "PORT", 8080)
        if port > 65535:
            raise ValueError("PORT must not exceed 65535")
        return cls(
            api_key=key,
            host=source.get("HOST", "0.0.0.0"),
            port=port,
            exchange_credentials=credentials,
            exchange_idle_ttl=_positive_int(source, "EXCHANGE_IDLE_TTL_SECONDS", 60),
            exchange_max_instances=_positive_int(source, "EXCHANGE_MAX_INSTANCES", 32),
            exchange_timeout_ms=_positive_int(source, "EXCHANGE_TIMEOUT_MS", 30_000),
            ws_idle_grace=_positive_int(source, "WS_IDLE_GRACE_SECONDS", 10),
            ws_max_clients=_positive_int(source, "WS_MAX_CLIENTS", 500),
            ws_max_streams=_positive_int(source, "WS_MAX_STREAMS", 1_000),
            cache_max_entries=_positive_int(source, "CACHE_MAX_ENTRIES", 10_000),
            cache_ticker_ttl=_positive_int(source, "CACHE_TICKER_TTL_SECONDS", 2),
            cache_orderbook_ttl=_positive_int(source, "CACHE_ORDERBOOK_TTL_SECONDS", 1),
            cache_trades_ttl=_positive_int(source, "CACHE_TRADES_TTL_SECONDS", 1),
            cache_ohlcv_ttl=_positive_int(source, "CACHE_OHLCV_TTL_SECONDS", 5),
            cache_markets_ttl=_positive_int(source, "CACHE_MARKETS_TTL_SECONDS", 300),
            rate_limit_requests=_positive_int(source, "RATE_LIMIT_REQUESTS", 120),
            rate_limit_window=_positive_int(source, "RATE_LIMIT_WINDOW_SECONDS", 60),
        )
