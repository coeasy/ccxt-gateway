from __future__ import annotations

import hashlib
import hmac
import json
import logging
import time
import uuid
from contextlib import asynccontextmanager
from typing import Annotated, Any

from fastapi import APIRouter, Depends, FastAPI, Header, HTTPException, Query, Request, WebSocket
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse, Response
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pydantic import BaseModel, ConfigDict, Field

from app.cache import AsyncTTLCache
from app.config import GatewayConfig
from app.exchange_manager import ExchangeManager, ExchangeManagerError
from app.history_store import HistoryStore
from app.idempotency import (
    IdempotencyCapacityError,
    IdempotencyConflictError,
    IdempotencyStore,
)
from app.market_stream_hub import MarketStreamHub
from app.metrics import RequestMetrics
from app.rate_limit import TokenBucketLimiter
from app.websocket import serve_websocket

logger = logging.getLogger("ccxt_gateway")
bearer_scheme = HTTPBearer(auto_error=False)
CAPABILITIES = {
    "load_markets": "loadMarkets",
    "fetch_ticker": "fetchTicker",
    "fetch_tickers": "fetchTickers",
    "fetch_order_book": "fetchOrderBook",
    "fetch_trades": "fetchTrades",
    "fetch_ohlcv": "fetchOHLCV",
    "fetch_funding_rate": "fetchFundingRate",
    "fetch_funding_rate_history": "fetchFundingRateHistory",
    "fetch_open_interest": "fetchOpenInterest",
    "fetch_open_interest_history": "fetchOpenInterestHistory",
    "fetch_mark_price": "fetchMarkPrice",
    "fetch_balance": "fetchBalance",
    "create_order": "createOrder",
    "cancel_order": "cancelOrder",
    "fetch_open_orders": "fetchOpenOrders",
    "fetch_order": "fetchOrder",
    "fetch_orders": "fetchOrders",
    "fetch_my_trades": "fetchMyTrades",
    "fetch_positions": "fetchPositions",
    "fetch_trading_fee": "fetchTradingFee",
}
REST_METHODS = {
    "markets": "load_markets",
    "ticker": "fetch_ticker",
    "tickers": "fetch_tickers",
    "orderbook": "fetch_order_book",
    "trades": "fetch_trades",
    "ohlcv": "fetch_ohlcv",
    "fundingRate": "fetch_funding_rate",
    "fundingRateHistory": "fetch_funding_rate_history",
    "openInterest": "fetch_open_interest",
    "openInterestHistory": "fetch_open_interest_history",
    "markPrice": "fetch_mark_price",
    "balance": "fetch_balance",
    "createOrder": "create_order",
    "cancelOrder": "cancel_order",
    "openOrders": "fetch_open_orders",
    "order": "fetch_order",
    "orders": "fetch_orders",
    "myTrades": "fetch_my_trades",
    "positions": "fetch_positions",
    "tradingFee": "fetch_trading_fee",
}
WEBSOCKET_METHODS = {
    "ticker": ("watch_ticker", "watchTicker"),
    "orderbook": ("watch_order_book", "watchOrderBook"),
    "trades": ("watch_trades", "watchTrades"),
    "ohlcv": ("watch_ohlcv", "watchOHLCV"),
    "fundingRate": ("watch_funding_rate", "watchFundingRate"),
    "markPrice": ("watch_mark_price", "watchMarkPrice"),
}


def _method_status(exchange: Any, method: str) -> str:
    if not callable(getattr(exchange, method, None)):
        return "unsupported"
    capability = getattr(exchange, "has", {}).get(CAPABILITIES.get(method, method))
    if capability is True:
        return "supported"
    return "emulated" if capability == "emulated" else "unsupported"


def _websocket_status(exchange: Any, method: str, capability: str) -> str:
    return (
        "supported"
        if callable(getattr(exchange, method, None))
        and getattr(exchange, "has", {}).get(capability) is True
        else "unsupported"
    )


def _parse_symbols(value: str) -> list[str]:
    symbols = list(dict.fromkeys(symbol.strip() for symbol in value.split(",")))
    invalid = (
        not symbols
        or len(symbols) > 50
        or any(not symbol or len(symbol) > 64 for symbol in symbols)
    )
    if invalid:
        raise ExchangeManagerError("Provide between 1 and 50 valid symbols", 400, "invalid_symbols")
    return symbols


def _market_types(markets: dict[str, Any]) -> list[str]:
    values = [market for market in markets.values() if isinstance(market, dict)]
    types = {market["type"] for market in values if market.get("type")}
    if any(market.get("margin") is True for market in values):
        types.add("margin")
    if any(
        market.get("contract") is True or market.get("type") in {"swap", "future", "option"}
        for market in values
    ):
        types.add("contract")
    return sorted(types)


def _filter_markets(
    markets: dict[str, Any], market_type: str | None, base: str | None, quote: str | None
) -> dict[str, Any]:
    return {
        symbol: market
        for symbol, market in markets.items()
        if isinstance(market, dict)
        and (
            market_type is None
            or (market_type == "margin" and market.get("margin") is True)
            or (
                market_type == "contract"
                and (
                    market.get("contract") is True
                    or market.get("type") in {"swap", "future", "option"}
                )
            )
            or (market_type not in {"margin", "contract"} and market.get("type") == market_type)
        )
        and (base is None or str(market.get("base", "")).upper() == base.upper())
        and (quote is None or str(market.get("quote", "")).upper() == quote.upper())
    }


def _pagination_params(
    paginate: bool,
    pagination_calls: int,
    since: int | None = None,
    forward_since: bool = False,
) -> dict[str, Any]:
    if not paginate:
        return {}
    params: dict[str, Any] = {"paginate": True, "paginationCalls": pagination_calls}
    if forward_since and since is not None:
        params["paginationDirection"] = "forward"
    return params


class OrderRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    symbol: str = Field(min_length=1, max_length=64)
    type: str = Field(pattern="^(limit|market)$")
    side: str = Field(pattern="^(buy|sell)$")
    amount: float = Field(gt=0, allow_inf_nan=False)
    price: float | None = Field(default=None, gt=0, allow_inf_nan=False)
    params: dict[str, Any] = Field(default_factory=dict)


def _load_ccxt() -> tuple[Any, Any]:
    try:
        import ccxt.async_support as ccxt_async
        import ccxt.pro as ccxt_pro
    except ImportError as exc:
        raise RuntimeError(
            "Install ccxt and a licensed CCXT Pro package to run the gateway"
        ) from exc
    return ccxt_async, ccxt_pro


def create_app(
    config: GatewayConfig | None = None,
    async_ccxt: Any | None = None,
    pro_ccxt: Any | None = None,
    history_store_backend: Any | None = None,
) -> FastAPI:
    config = config or GatewayConfig.from_env()
    if async_ccxt is None or pro_ccxt is None:
        default_async, default_pro = _load_ccxt()
        async_ccxt = async_ccxt or default_async
        pro_ccxt = pro_ccxt or default_pro

    exchange_manager = ExchangeManager(
        async_ccxt,
        pro_ccxt,
        credentials=config.exchange_credentials,
        idle_ttl=config.exchange_idle_ttl,
        max_instances=config.exchange_max_instances,
        timeout_ms=config.exchange_timeout_ms,
    )
    cache = AsyncTTLCache(max_entries=config.cache_max_entries)
    limiter = TokenBucketLimiter(config.rate_limit_requests, config.rate_limit_window)
    stream_hub = MarketStreamHub(exchange_manager, config.ws_idle_grace, config.ws_max_streams)

    @asynccontextmanager
    async def lifespan(_: FastAPI):
        try:
            await history_store.initialize()
            yield
        finally:
            await stream_hub.close()
            await exchange_manager.close()
            await cache.clear()
            await history_store.close()

    history_store = history_store_backend or HistoryStore(config.database_url)
    metrics = RequestMetrics()
    idempotency = IdempotencyStore()

    app = FastAPI(
        title="CCXT Gateway",
        version="0.1.0",
        description=(
            "Unified CCXT REST and WebSocket gateway. REST APIs use bearer authentication; "
            "WebSockets authenticate in their first message."
        ),
        lifespan=lifespan,
        docs_url="/docs",
        redoc_url=None,
        openapi_url="/openapi.json",
    )
    app.state.exchange_manager = exchange_manager
    app.state.stream_hub = stream_hub
    app.state.cache = cache
    app.state.ws_clients = set()
    app.state.metrics = metrics
    app.state.history_store = history_store

    @app.middleware("http")
    async def gateway_rate_limit(request: Request, call_next: Any):
        started = time.perf_counter()
        request.state.request_id = uuid.uuid4().hex
        path = request.url.path
        response = None
        if path.startswith("/v1/"):
            client = request.client.host if request.client else "unknown"
            allowed, retry_after = limiter.consume(client)
            if not allowed:
                response = JSONResponse(
                    {"error": "rate_limit_exceeded", "message": "Gateway request limit exceeded"},
                    status_code=429,
                    headers={"Retry-After": str(retry_after)},
                )
        if response is None:
            response = await call_next(request)
        route = getattr(request.scope.get("route"), "path", "/unmatched")
        metrics.observe(request.method, route, response.status_code, time.perf_counter() - started)
        response.headers["X-Request-ID"] = request.state.request_id
        return response

    async def authenticate(
        credentials: Annotated[HTTPAuthorizationCredentials | None, Depends(bearer_scheme)],
    ) -> None:
        token = (
            credentials.credentials
            if credentials and credentials.scheme.lower() == "bearer"
            else ""
        )
        if not hmac.compare_digest(token.encode(), config.api_key.encode()):
            raise HTTPException(
                status_code=401,
                detail={"error": "unauthorized", "message": "A valid bearer token is required"},
            )

    router = APIRouter(prefix="/v1", dependencies=[Depends(authenticate)])

    @app.get("/version")
    async def version() -> dict[str, str]:
        return {"version": "0.1.0", "commit": config.git_sha}

    @app.get("/metrics", dependencies=[Depends(authenticate)])
    async def prometheus_metrics() -> Any:
        return Response(
            metrics.render(cache.size, len(exchange_manager.entries), len(stream_hub.streams)),
            media_type="text/plain; version=0.0.4; charset=utf-8",
        )

    async def call_exchange(
        exchange_id: str,
        method: str,
        args: tuple[Any, ...],
        ttl: float | None = None,
        market_type: str | None = None,
    ) -> Any:
        async def operation(exchange: Any) -> Any:
            if market_type == "contract":
                markets = await exchange.load_markets()
                market = markets.get(args[0])
                if not market or market.get("type") not in {"swap", "future"}:
                    raise ExchangeManagerError(
                        "The symbol is not an available swap or future market",
                        400,
                        "invalid_market_type",
                    )
            capability = getattr(exchange, "has", {})
            supported = capability.get(CAPABILITIES.get(method, method))
            if not (supported is True or supported == "emulated") or not callable(
                getattr(exchange, method, None)
            ):
                raise ExchangeManagerError(
                    f"Exchange does not support {method}", 501, "method_not_supported"
                )
            return await getattr(exchange, method)(*args)

        try:
            if ttl is None:
                return await exchange_manager.with_exchange(exchange_id, operation)
            key = json.dumps(
                [exchange_id, method, args, market_type], separators=(",", ":"), sort_keys=True
            )
            return await cache.get_or_create(
                key, ttl, lambda: exchange_manager.with_exchange(exchange_id, operation)
            )
        except ExchangeManagerError:
            raise
        except Exception:
            metrics.exchange_errors += 1
            raise

    @router.get("/exchanges")
    async def exchanges() -> dict[str, list[str]]:
        return {"exchanges": exchange_manager.list_exchanges()}

    @router.get("/{exchange}/markets")
    async def markets(
        exchange: str,
        market_type: str | None = Query(
            default=None, alias="type", pattern="^(spot|margin|swap|future|option|contract)$"
        ),
        base: str | None = Query(default=None, min_length=1, max_length=16),
        quote: str | None = Query(default=None, min_length=1, max_length=16),
    ) -> dict[str, Any]:
        try:
            loaded = await call_exchange(exchange, "load_markets", (), config.cache_markets_ttl)
            return {
                "exchange": exchange,
                "filters": {"type": market_type, "base": base, "quote": quote},
                "markets": _filter_markets(loaded, market_type, base, quote),
            }
        except ExchangeManagerError:
            raise
        except Exception as exc:
            _raise_exchange_error(exchange, exc)

    @router.get("/{exchange}/capabilities")
    async def capabilities(exchange: str) -> dict[str, Any]:
        rest_lease = None
        pro_lease = None
        try:
            rest_lease = await exchange_manager.acquire(exchange, "rest")
            pro_lease = await exchange_manager.acquire(exchange, "pro")
            rest_exchange = rest_lease.exchange
            pro_exchange = pro_lease.exchange
            markets = await rest_exchange.load_markets()
            return {
                "exchange": exchange,
                "marketCount": len(markets),
                "marketTypes": _market_types(markets),
                "timeframes": sorted((getattr(rest_exchange, "timeframes", None) or {}).keys()),
                "rest": {
                    name: _method_status(rest_exchange, method)
                    for name, method in REST_METHODS.items()
                },
                "websocket": {
                    name: _websocket_status(pro_exchange, method, capability)
                    for name, (method, capability) in WEBSOCKET_METHODS.items()
                },
            }
        except ExchangeManagerError:
            raise
        except Exception as exc:
            _raise_exchange_error(exchange, exc)
        finally:
            if pro_lease is not None:
                await pro_lease.release()
            if rest_lease is not None:
                await rest_lease.release()

    @router.get("/{exchange}/ticker")
    async def ticker(
        exchange: str, symbol: str = Query(min_length=1, max_length=64)
    ) -> dict[str, Any]:
        try:
            return {
                "exchange": exchange,
                "symbol": symbol,
                "ticker": await call_exchange(
                    exchange, "fetch_ticker", (symbol,), config.cache_ticker_ttl
                ),
            }
        except ExchangeManagerError:
            raise
        except Exception as exc:
            _raise_exchange_error(exchange, exc)

    @router.get("/{exchange}/tickers")
    async def tickers(
        exchange: str, symbols: str = Query(min_length=3, max_length=3249)
    ) -> dict[str, Any]:
        try:
            requested = _parse_symbols(symbols)
            value = await call_exchange(
                exchange,
                "fetch_tickers",
                (requested,),
                config.cache_ticker_ttl,
            )
            return {"exchange": exchange, "symbols": requested, "tickers": value}
        except ExchangeManagerError:
            raise
        except Exception as exc:
            _raise_exchange_error(exchange, exc)

    @router.get("/{exchange}/orderbook")
    async def orderbook(
        exchange: str,
        symbol: str = Query(min_length=1, max_length=64),
        limit: int = Query(default=20, ge=1, le=1000),
    ) -> dict[str, Any]:
        try:
            value = await call_exchange(
                exchange, "fetch_order_book", (symbol, limit), config.cache_orderbook_ttl
            )
            return {"exchange": exchange, "symbol": symbol, "orderbook": value}
        except ExchangeManagerError:
            raise
        except Exception as exc:
            _raise_exchange_error(exchange, exc)

    @router.get("/archive/ohlcv")
    async def archived_ohlcv(
        exchange: str = Query(min_length=1, max_length=64, pattern="^[a-z0-9]+$"),
        symbol: str = Query(min_length=1, max_length=64),
        timeframe: str = Query(min_length=1, max_length=16),
        price: str = Query(default="last", pattern="^(last|mark|index|premiumIndex)$"),
        since: int | None = Query(default=None, ge=0),
        until: int | None = Query(default=None, ge=0),
        limit: int = Query(default=1000, ge=1, le=1000),
    ) -> dict[str, Any]:
        if since is not None and until is not None and until < since:
            raise HTTPException(
                400,
                detail={
                    "error": "invalid_time_range",
                    "message": "until must be greater than or equal to since",
                },
            )
        try:
            rows = await history_store.query(
                "ohlcv", exchange, symbol, f"{timeframe}:{price}", since, until, limit
            )
            if rows is None:
                raise HTTPException(
                    503,
                    detail={
                        "error": "storage_not_configured",
                        "message": "Set DATABASE_URL to enable archived market data",
                    },
                )
            return {
                "source": "archive",
                "exchange": exchange,
                "symbol": symbol,
                "timeframe": timeframe,
                "price": price,
                "candles": rows,
            }
        except HTTPException:
            raise
        except Exception as exc:
            metrics.storage_errors += 1
            logger.exception("Historical candle query failed")
            raise HTTPException(
                503,
                detail={
                    "error": "storage_unavailable",
                    "message": "Historical storage is temporarily unavailable",
                },
            ) from exc

    @router.get("/archive/trades")
    async def archived_trades(
        exchange: str = Query(min_length=1, max_length=64, pattern="^[a-z0-9]+$"),
        symbol: str = Query(min_length=1, max_length=64),
        since: int | None = Query(default=None, ge=0),
        until: int | None = Query(default=None, ge=0),
        limit: int = Query(default=1000, ge=1, le=1000),
    ) -> dict[str, Any]:
        if since is not None and until is not None and until < since:
            raise HTTPException(
                400,
                detail={
                    "error": "invalid_time_range",
                    "message": "until must be greater than or equal to since",
                },
            )
        try:
            rows = await history_store.query("trades", exchange, symbol, "", since, until, limit)
            if rows is None:
                raise HTTPException(
                    503,
                    detail={
                        "error": "storage_not_configured",
                        "message": "Set DATABASE_URL to enable archived market data",
                    },
                )
            return {"source": "archive", "exchange": exchange, "symbol": symbol, "trades": rows}
        except HTTPException:
            raise
        except Exception as exc:
            metrics.storage_errors += 1
            logger.exception("Historical trade query failed")
            raise HTTPException(
                503,
                detail={
                    "error": "storage_unavailable",
                    "message": "Historical storage is temporarily unavailable",
                },
            ) from exc

    @router.get("/{exchange}/trades")
    async def trades(
        exchange: str,
        symbol: str = Query(min_length=1, max_length=64),
        since: int | None = Query(default=None, ge=0),
        limit: int = Query(default=100, ge=1, le=1000),
        paginate: bool = Query(default=False),
        pagination_calls: int = Query(default=5, alias="paginationCalls", ge=1, le=5),
    ) -> dict[str, Any]:
        try:
            value = await call_exchange(
                exchange,
                "fetch_trades",
                (
                    symbol,
                    since,
                    limit,
                    _pagination_params(paginate, pagination_calls, since, forward_since=True),
                ),
                config.cache_trades_ttl,
            )
            try:
                await history_store.save_trades(exchange, symbol, value)
            except Exception:
                metrics.storage_errors += 1
                logger.exception("Failed to archive exchange trades")
            return {"exchange": exchange, "symbol": symbol, "trades": value}
        except ExchangeManagerError:
            raise
        except Exception as exc:
            _raise_exchange_error(exchange, exc)

    @router.get("/{exchange}/ohlcv")
    async def ohlcv(
        exchange: str,
        symbol: str = Query(min_length=1, max_length=64),
        timeframe: str = Query(min_length=1, max_length=16),
        since: int | None = Query(default=None, ge=0),
        limit: int = Query(default=100, ge=1, le=1000),
        price: str | None = Query(default=None, pattern="^(mark|index|premiumIndex)$"),
        paginate: bool = Query(default=False),
        pagination_calls: int = Query(default=5, alias="paginationCalls", ge=1, le=5),
    ) -> dict[str, Any]:
        try:
            value = await call_exchange(
                exchange,
                "fetch_ohlcv",
                (
                    symbol,
                    timeframe,
                    since,
                    limit,
                    {
                        **({"price": price} if price else {}),
                        **_pagination_params(paginate, pagination_calls, since),
                    },
                ),
                config.cache_ohlcv_ttl,
                market_type="contract" if price else None,
            )
            try:
                await history_store.save_candles(
                    exchange, symbol, timeframe, value, price or "last"
                )
            except Exception:
                metrics.storage_errors += 1
                logger.exception("Failed to archive exchange candles")
            return {
                "exchange": exchange,
                "symbol": symbol,
                "timeframe": timeframe,
                "price": price or "last",
                "candles": value,
            }
        except ExchangeManagerError:
            raise
        except Exception as exc:
            _raise_exchange_error(exchange, exc)

    @router.get("/{exchange}/funding-rate")
    async def funding_rate(
        exchange: str, symbol: str = Query(min_length=1, max_length=64)
    ) -> dict[str, Any]:
        try:
            value = await call_exchange(
                exchange,
                "fetch_funding_rate",
                (symbol,),
                config.cache_ticker_ttl,
                market_type="contract",
            )
            return {"exchange": exchange, "symbol": symbol, "fundingRate": value}
        except ExchangeManagerError:
            raise
        except Exception as exc:
            _raise_exchange_error(exchange, exc)

    @router.get("/{exchange}/funding-rate-history")
    async def funding_rate_history(
        exchange: str,
        symbol: str = Query(min_length=1, max_length=64),
        since: int | None = Query(default=None, ge=0),
        limit: int | None = Query(default=None, ge=1, le=1000),
        paginate: bool = Query(default=False),
        pagination_calls: int = Query(default=5, alias="paginationCalls", ge=1, le=5),
    ) -> dict[str, Any]:
        try:
            value = await call_exchange(
                exchange,
                "fetch_funding_rate_history",
                (symbol, since, limit, _pagination_params(paginate, pagination_calls, since)),
                market_type="contract",
            )
            return {"exchange": exchange, "symbol": symbol, "fundingRates": value}
        except ExchangeManagerError:
            raise
        except Exception as exc:
            _raise_exchange_error(exchange, exc)

    @router.get("/{exchange}/open-interest")
    async def open_interest(
        exchange: str, symbol: str = Query(min_length=1, max_length=64)
    ) -> dict[str, Any]:
        try:
            value = await call_exchange(
                exchange,
                "fetch_open_interest",
                (symbol,),
                config.cache_ticker_ttl,
                market_type="contract",
            )
            return {"exchange": exchange, "symbol": symbol, "openInterest": value}
        except ExchangeManagerError:
            raise
        except Exception as exc:
            _raise_exchange_error(exchange, exc)

    @router.get("/{exchange}/open-interest-history")
    async def open_interest_history(
        exchange: str,
        symbol: str = Query(min_length=1, max_length=64),
        timeframe: str = Query(default="5m", min_length=1, max_length=16),
        since: int | None = Query(default=None, ge=0),
        limit: int | None = Query(default=None, ge=1, le=1000),
        paginate: bool = Query(default=False),
        pagination_calls: int = Query(default=5, alias="paginationCalls", ge=1, le=5),
    ) -> dict[str, Any]:
        try:
            value = await call_exchange(
                exchange,
                "fetch_open_interest_history",
                (
                    symbol,
                    timeframe,
                    since,
                    limit,
                    _pagination_params(paginate, pagination_calls, since),
                ),
                market_type="contract",
            )
            return {
                "exchange": exchange,
                "symbol": symbol,
                "timeframe": timeframe,
                "history": value,
            }
        except ExchangeManagerError:
            raise
        except Exception as exc:
            _raise_exchange_error(exchange, exc)

    @router.get("/{exchange}/mark-price")
    async def mark_price(
        exchange: str, symbol: str = Query(min_length=1, max_length=64)
    ) -> dict[str, Any]:
        try:
            value = await call_exchange(
                exchange,
                "fetch_mark_price",
                (symbol,),
                config.cache_ticker_ttl,
                market_type="contract",
            )
            return {"exchange": exchange, "symbol": symbol, "markPrice": value}
        except ExchangeManagerError:
            raise
        except Exception as exc:
            _raise_exchange_error(exchange, exc)

    @router.get("/{exchange}/mark-price-ohlcv")
    async def mark_price_ohlcv(
        exchange: str,
        symbol: str = Query(min_length=1, max_length=64),
        timeframe: str = Query(min_length=1, max_length=16),
        since: int | None = Query(default=None, ge=0),
        limit: int = Query(default=100, ge=1, le=1000),
        price: str = Query(default="mark", pattern="^mark$"),
        paginate: bool = Query(default=False),
        pagination_calls: int = Query(default=5, alias="paginationCalls", ge=1, le=5),
    ) -> dict[str, Any]:
        try:
            value = await call_exchange(
                exchange,
                "fetch_ohlcv",
                (
                    symbol,
                    timeframe,
                    since,
                    limit,
                    {
                        "price": "mark",
                        **_pagination_params(paginate, pagination_calls, since),
                    },
                ),
                market_type="contract",
            )
            return {
                "exchange": exchange,
                "symbol": symbol,
                "timeframe": timeframe,
                "price": price,
                "candles": value,
            }
        except ExchangeManagerError:
            raise
        except Exception as exc:
            _raise_exchange_error(exchange, exc)

    @router.get("/{exchange}/balance")
    async def balance(exchange: str) -> dict[str, Any]:
        if not exchange_manager.has_credentials(exchange):
            raise HTTPException(
                403,
                detail={
                    "error": "credentials_not_configured",
                    "message": "No exchange credentials are configured",
                },
            )
        try:
            return {
                "exchange": exchange,
                "balance": await call_exchange(exchange, "fetch_balance", ()),
            }
        except ExchangeManagerError:
            raise
        except Exception as exc:
            _raise_exchange_error(exchange, exc)

    async def private_call(
        exchange: str, method: str, args: tuple[Any, ...], response_key: str
    ) -> dict[str, Any]:
        if not exchange_manager.has_credentials(exchange):
            raise HTTPException(
                403,
                detail={
                    "error": "credentials_not_configured",
                    "message": "No exchange credentials are configured",
                },
            )
        try:
            return {"exchange": exchange, response_key: await call_exchange(exchange, method, args)}
        except ExchangeManagerError:
            raise
        except Exception as exc:
            _raise_exchange_error(exchange, exc)

    @router.get("/{exchange}/open-orders")
    async def open_orders(
        exchange: str, symbol: str | None = Query(default=None, min_length=1, max_length=64)
    ) -> dict[str, Any]:
        return await private_call(exchange, "fetch_open_orders", (symbol,), "orders")

    @router.get("/{exchange}/order/{order_id}")
    async def fetch_order(
        exchange: str, order_id: str, symbol: str = Query(min_length=1, max_length=64)
    ) -> dict[str, Any]:
        if len(order_id) > 128:
            raise HTTPException(
                400, detail={"error": "invalid_order_id", "message": "Order ID is too long"}
            )
        return await private_call(exchange, "fetch_order", (order_id, symbol), "order")

    @router.get("/{exchange}/orders/history")
    async def order_history(
        exchange: str,
        symbol: str = Query(min_length=1, max_length=64),
        since: int | None = Query(default=None, ge=0),
        limit: int = Query(default=100, ge=1, le=1000),
    ) -> dict[str, Any]:
        return await private_call(exchange, "fetch_orders", (symbol, since, limit), "orders")

    @router.get("/{exchange}/my-trades")
    async def my_trades(
        exchange: str,
        symbol: str = Query(min_length=1, max_length=64),
        since: int | None = Query(default=None, ge=0),
        limit: int = Query(default=100, ge=1, le=1000),
    ) -> dict[str, Any]:
        return await private_call(exchange, "fetch_my_trades", (symbol, since, limit), "trades")

    @router.get("/{exchange}/positions")
    async def positions(
        exchange: str, symbols: str | None = Query(default=None, min_length=1, max_length=3249)
    ) -> dict[str, Any]:
        parsed = _parse_symbols(symbols) if symbols else None
        return await private_call(exchange, "fetch_positions", (parsed,), "positions")

    @router.get("/{exchange}/trading-fee")
    async def trading_fee(
        exchange: str, symbol: str = Query(min_length=1, max_length=64)
    ) -> dict[str, Any]:
        return await private_call(exchange, "fetch_trading_fee", (symbol,), "fee")

    @router.post("/{exchange}/order", status_code=201)
    async def create_order(
        exchange: str,
        request: Request,
        body: OrderRequest,
        idempotency_key: str = Header(
            alias="Idempotency-Key", min_length=8, max_length=128, pattern="^[A-Za-z0-9._:-]+$"
        ),
    ) -> dict[str, Any]:
        if not exchange_manager.has_credentials(exchange):
            raise HTTPException(
                403,
                detail={
                    "error": "credentials_not_configured",
                    "message": "No exchange credentials are configured",
                },
            )
        if not config.trading_enabled:
            raise HTTPException(
                403,
                detail={
                    "error": "trading_disabled",
                    "message": "Set TRADING_ENABLED=true to enable order submission",
                },
            )
        if body.type == "market" and not config.market_orders_enabled:
            raise HTTPException(
                403,
                detail={
                    "error": "market_orders_disabled",
                    "message": "Set MARKET_ORDERS_ENABLED=true to enable market orders",
                },
            )
        if body.type == "limit" and body.price is None:
            raise HTTPException(
                400, detail={"error": "price_required", "message": "Limit orders require a price"}
            )
        try:
            payload = body.model_dump(mode="json")

            async def submit_and_audit() -> Any:
                result = await call_exchange(
                    exchange,
                    "create_order",
                    (body.symbol, body.type, body.side, body.amount, body.price, body.params),
                )
                key_hash = IdempotencyStore.fingerprint(idempotency_key)
                audit_payload = {
                    "symbol": body.symbol,
                    "type": body.type,
                    "side": body.side,
                    "amount": body.amount,
                    "price": body.price,
                    "requestId": getattr(request.state, "request_id", "unknown"),
                }
                logger.info(
                    "order.created exchange=%s order_id=%s idempotency_hash=%s request_id=%s "
                    "symbol=%s side=%s type=%s amount=%s",
                    exchange,
                    result.get("id") if isinstance(result, dict) else None,
                    key_hash,
                    audit_payload["requestId"],
                    body.symbol,
                    body.side,
                    body.type,
                    body.amount,
                )
                try:
                    await history_store.audit(
                        "order.created",
                        exchange,
                        str(result.get("id"))
                        if isinstance(result, dict) and result.get("id") is not None
                        else None,
                        key_hash,
                        audit_payload,
                    )
                except Exception:
                    metrics.storage_errors += 1
                    logger.exception("Persistent order audit write failed")
                return result

            result = await idempotency.execute(
                f"{exchange}:{idempotency_key}", payload, submit_and_audit
            )
            return {"exchange": exchange, "order": result}
        except ExchangeManagerError:
            raise
        except IdempotencyCapacityError:
            raise
        except IdempotencyConflictError:
            raise
        except Exception as exc:
            _raise_exchange_error(exchange, exc)

    @router.delete("/{exchange}/order/{order_id}")
    async def cancel_order(
        exchange: str,
        order_id: str,
        symbol: str = Query(min_length=1, max_length=64),
    ) -> dict[str, Any]:
        if len(order_id) > 128:
            raise HTTPException(422, detail="order id is too long")
        if not exchange_manager.has_credentials(exchange):
            raise HTTPException(
                403,
                detail={
                    "error": "credentials_not_configured",
                    "message": "No exchange credentials are configured",
                },
            )
        try:
            result = await call_exchange(exchange, "cancel_order", (order_id, symbol))
            key_hash = hashlib.sha256(f"cancel:{exchange}:{order_id}:{symbol}".encode()).hexdigest()
            try:
                await history_store.audit(
                    "order.cancelled", exchange, order_id, key_hash, {"symbol": symbol}
                )
            except Exception:
                metrics.storage_errors += 1
                logger.exception("Persistent order audit write failed")
            logger.info(
                "order.cancelled exchange=%s order_id=%s symbol=%s", exchange, order_id, symbol
            )
            return {"exchange": exchange, "order": result}
        except ExchangeManagerError:
            raise
        except Exception as exc:
            _raise_exchange_error(exchange, exc)

    @app.exception_handler(ExchangeManagerError)
    async def exchange_error_handler(_: Request, exc: ExchangeManagerError) -> JSONResponse:
        return JSONResponse({"error": exc.code, "message": str(exc)}, status_code=exc.status_code)

    @app.exception_handler(IdempotencyConflictError)
    async def idempotency_error_handler(_: Request, exc: IdempotencyConflictError) -> JSONResponse:
        return JSONResponse({"error": "idempotency_conflict", "message": str(exc)}, status_code=409)

    @app.exception_handler(IdempotencyCapacityError)
    async def idempotency_capacity_handler(
        _: Request, exc: IdempotencyCapacityError
    ) -> JSONResponse:
        return JSONResponse(
            {"error": "idempotency_capacity_reached", "message": str(exc)}, status_code=503
        )

    @app.exception_handler(HTTPException)
    async def http_error_handler(_: Request, exc: HTTPException) -> JSONResponse:
        detail = exc.detail
        if (
            isinstance(detail, dict)
            and isinstance(detail.get("error"), str)
            and isinstance(detail.get("message"), str)
        ):
            body = {"error": detail["error"], "message": detail["message"]}
        else:
            code = {
                401: "unauthorized",
                403: "forbidden",
                404: "not_found",
                429: "rate_limit_exceeded",
            }.get(exc.status_code, "request_failed")
            body = {
                "error": code,
                "message": detail if isinstance(detail, str) else "The request failed",
            }
        return JSONResponse(body, status_code=exc.status_code, headers=exc.headers)

    @app.exception_handler(RequestValidationError)
    async def validation_error_handler(_: Request, exc: RequestValidationError) -> JSONResponse:
        details = [
            {
                "path": ".".join(str(part) for part in error.get("loc", ())),
                "message": error.get("msg"),
                "code": error.get("type"),
            }
            for error in exc.errors()
        ]
        return JSONResponse(
            {
                "error": "validation_error",
                "message": "Request validation failed",
                "details": details,
            },
            status_code=422,
        )

    @app.exception_handler(Exception)
    async def unexpected_error_handler(request: Request, exc: Exception) -> JSONResponse:
        logger.exception(
            "Unhandled request failure (request_id=%s)",
            getattr(request.state, "request_id", "unknown"),
        )
        return JSONResponse(
            {"error": "internal_error", "message": "The request failed"}, status_code=500
        )

    @app.get("/health/live")
    async def health_live() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/health/ready")
    async def health_ready() -> dict[str, str]:
        if exchange_manager.closed:
            raise HTTPException(503, detail="shutting down")
        if not await history_store.ping():
            raise HTTPException(
                503,
                detail={
                    "error": "storage_unavailable",
                    "message": "Historical storage is unavailable",
                },
            )
        return {"status": "ready", "storage": "connected" if history_store.enabled else "disabled"}

    app.include_router(router)

    @app.websocket("/ws")
    async def websocket_endpoint(websocket: WebSocket) -> None:
        await serve_websocket(websocket, stream_hub, config.api_key, config.ws_max_clients, limiter)

    return app


def _raise_exchange_error(exchange: str, error: Exception) -> None:
    logger.warning("Exchange request failed (%s): %s", exchange, type(error).__name__)
    raise HTTPException(
        502, detail={"error": "exchange_request_failed", "message": "The exchange request failed"}
    ) from error
