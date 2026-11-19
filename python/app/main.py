from __future__ import annotations

import hmac
import json
import logging
from contextlib import asynccontextmanager
from typing import Any

from fastapi import APIRouter, Depends, FastAPI, Header, HTTPException, Query, Request, WebSocket
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict, Field

from app.cache import AsyncTTLCache
from app.config import GatewayConfig
from app.exchange_manager import ExchangeManager, ExchangeManagerError
from app.market_stream_hub import MarketStreamHub
from app.rate_limit import TokenBucketLimiter
from app.websocket import serve_websocket

logger = logging.getLogger("ccxt_gateway")
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
    invalid = not symbols or len(symbols) > 50 or any(
        not symbol or len(symbol) > 64 for symbol in symbols
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
        market.get("contract") is True
        or market.get("type") in {"swap", "future", "option"}
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
            yield
        finally:
            await stream_hub.close()
            await exchange_manager.close()
            await cache.clear()

    app = FastAPI(
        title="CCXT Gateway",
        version="0.1.0",
        lifespan=lifespan,
        docs_url=None,
        redoc_url=None,
        openapi_url=None,
    )
    app.state.exchange_manager = exchange_manager
    app.state.stream_hub = stream_hub
    app.state.cache = cache
    app.state.ws_clients = set()

    @app.middleware("http")
    async def gateway_rate_limit(request: Request, call_next: Any):
        path = request.url.path
        if path.startswith("/v1/"):
            client = request.client.host if request.client else "unknown"
            allowed, retry_after = limiter.consume(client)
            if not allowed:
                return JSONResponse(
                    {"error": "rate_limit_exceeded", "message": "Gateway request limit exceeded"},
                    status_code=429,
                    headers={"Retry-After": str(retry_after)},
                )
        return await call_next(request)

    async def authenticate(authorization: str | None = Header(default=None)) -> None:
        token = authorization[7:] if authorization and authorization.startswith("Bearer ") else ""
        if not hmac.compare_digest(token.encode(), config.api_key.encode()):
            raise HTTPException(
                status_code=401,
                detail={"error": "unauthorized", "message": "A valid bearer token is required"},
            )

    router = APIRouter(prefix="/v1", dependencies=[Depends(authenticate)])

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
            if (
                not (supported is True or supported == "emulated")
                or not callable(getattr(exchange, method, None))
            ):
                raise ExchangeManagerError(
                    f"Exchange does not support {method}", 501, "method_not_supported"
                )
            return await getattr(exchange, method)(*args)

        if ttl is None:
            return await exchange_manager.with_exchange(exchange_id, operation)
        key = json.dumps(
            [exchange_id, method, args, market_type], separators=(",", ":"), sort_keys=True
        )
        return await cache.get_or_create(
            key, ttl, lambda: exchange_manager.with_exchange(exchange_id, operation)
        )

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
            loaded = await call_exchange(
                exchange, "load_markets", (), config.cache_markets_ttl
            )
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

    @router.post("/{exchange}/order", status_code=201)
    async def create_order(exchange: str, body: OrderRequest) -> dict[str, Any]:
        if not exchange_manager.has_credentials(exchange):
            raise HTTPException(
                403,
                detail={
                    "error": "credentials_not_configured",
                    "message": "No exchange credentials are configured",
                },
            )
        if body.type == "limit" and body.price is None:
            raise HTTPException(
                400, detail={"error": "price_required", "message": "Limit orders require a price"}
            )
        try:
            result = await call_exchange(
                exchange,
                "create_order",
                (body.symbol, body.type, body.side, body.amount, body.price, body.params),
            )
            return {"exchange": exchange, "order": result}
        except ExchangeManagerError:
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
            return {
                "exchange": exchange,
                "order": await call_exchange(exchange, "cancel_order", (order_id, symbol)),
            }
        except ExchangeManagerError:
            raise
        except Exception as exc:
            _raise_exchange_error(exchange, exc)

    @app.exception_handler(ExchangeManagerError)
    async def exchange_error_handler(_: Request, exc: ExchangeManagerError) -> JSONResponse:
        return JSONResponse({"error": exc.code, "message": str(exc)}, status_code=exc.status_code)

    @app.get("/health/live")
    async def health_live() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/health/ready")
    async def health_ready() -> dict[str, str]:
        if exchange_manager.closed:
            raise HTTPException(503, detail="shutting down")
        return {"status": "ready"}

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
