from dataclasses import replace

from fastapi.testclient import TestClient

from app.main import create_app


def headers(config):
    return {"Authorization": f"Bearer {config.api_key}"}


def test_rest_auth_cache_private_guard_and_order(exchange_modules, gateway_config):
    rest, pro, metrics = exchange_modules
    config = replace(gateway_config, trading_enabled=True, market_orders_enabled=True)
    app = create_app(config, rest, pro)
    with TestClient(app) as client:
        assert client.get("/health/ready").json() == {"status": "ready", "storage": "disabled"}
        assert client.get("/v1/exchanges").status_code == 401
        response = client.get(
            "/v1/binance/ticker", params={"symbol": "BTC/USDT"}, headers=headers(gateway_config)
        )
        assert response.status_code == 200
        assert response.json()["ticker"]["last"] == 42.5
        client.get(
            "/v1/binance/ticker", params={"symbol": "BTC/USDT"}, headers=headers(gateway_config)
        )
        assert metrics["ticker_calls"] == 1
        assert client.get("/v1/kraken/balance", headers=headers(gateway_config)).status_code == 403
        placed = client.post(
            "/v1/binance/order",
            headers={**headers(config), "Idempotency-Key": "test-order-0001"},
            json={"symbol": "BTC/USDT", "type": "limit", "side": "buy", "amount": 1, "price": 10},
        )
        assert placed.status_code == 201
        assert metrics["order_calls"] == 1
        assert (
            client.post(
                "/v1/binance/order",
                headers={**headers(config), "Idempotency-Key": "test-order-0002"},
                json={"symbol": "BTC/USDT", "type": "limit", "side": "buy", "amount": 1},
            ).status_code
            == 400
        )


def test_rate_limit_applies_to_gateway_routes(exchange_modules, gateway_config):
    rest, pro, _ = exchange_modules
    config = replace(gateway_config, rate_limit_requests=1)
    app = create_app(config, rest, pro)
    with TestClient(app) as client:
        first = client.get("/v1/exchanges", headers=headers(config))
        second = client.get("/v1/exchanges", headers=headers(config))
        assert first.status_code == 200
        assert second.status_code == 429
        assert "Retry-After" in second.headers


def test_market_filters_history_and_contract_metrics(exchange_modules, gateway_config):
    rest, pro, _ = exchange_modules
    app = create_app(gateway_config, rest, pro)
    auth = headers(gateway_config)
    with TestClient(app) as client:
        markets = client.get(
            "/v1/binance/markets",
            params={"type": "swap", "base": "btc", "quote": "usdt"},
            headers=auth,
        )
        contracts = client.get("/v1/binance/markets", params={"type": "contract"}, headers=auth)
        margin_markets = client.get("/v1/binance/markets", params={"type": "margin"}, headers=auth)
        trades = client.get(
            "/v1/binance/trades",
            params={
                "symbol": "BTC/USDT:USDT",
                "since": 1700000000000,
                "limit": 50,
                "paginate": "true",
                "paginationCalls": 3,
            },
            headers=auth,
        )
        funding = client.get(
            "/v1/binance/funding-rate-history",
            params={
                "symbol": "BTC/USDT:USDT",
                "since": 1700000000000,
                "limit": 20,
                "paginate": "true",
                "paginationCalls": 3,
            },
            headers=auth,
        )
        spot_funding = client.get(
            "/v1/binance/funding-rate",
            params={"symbol": "BTC/USDT"},
            headers=auth,
        )
        interest = client.get(
            "/v1/binance/open-interest-history",
            params={"symbol": "BTC/USDT:USDT", "timeframe": "1h", "since": 1700000000000},
            headers=auth,
        )
        mark_price = client.get(
            "/v1/binance/mark-price",
            params={"symbol": "BTC/USDT:USDT"},
            headers=auth,
        )
        mark_candles = client.get(
            "/v1/binance/mark-price-ohlcv",
            params={
                "symbol": "BTC/USDT:USDT",
                "timeframe": "1h",
                "since": 1700000000000,
                "paginate": "true",
                "paginationCalls": 2,
            },
            headers=auth,
        )
        index_candles = client.get(
            "/v1/binance/ohlcv",
            params={
                "symbol": "BTC/USDT:USDT",
                "timeframe": "1h",
                "price": "index",
            },
            headers=auth,
        )

    assert list(markets.json()["markets"]) == ["BTC/USDT:USDT"]
    assert list(contracts.json()["markets"]) == ["BTC/USDT:USDT"]
    assert list(margin_markets.json()["markets"]) == ["BTC/USDT"]
    assert trades.json()["trades"][0]["since"] == 1700000000000
    assert trades.json()["trades"][0]["paginate"] is True
    assert trades.json()["trades"][0]["paginationCalls"] == 3
    assert trades.json()["trades"][0]["paginationDirection"] == "forward"
    assert funding.json()["fundingRates"][0]["limit"] == 20
    assert funding.json()["fundingRates"][0]["paginationCalls"] == 3
    assert spot_funding.status_code == 400
    assert spot_funding.json()["error"] == "invalid_market_type"
    assert interest.json()["history"][0]["timeframe"] == "1h"
    assert mark_price.json()["markPrice"]["markPrice"] == 123.4
    assert mark_candles.json()["candles"][0][8] is True
    assert mark_candles.json()["candles"][0][9] == "mark"
    assert index_candles.json()["price"] == "index"
    assert index_candles.json()["candles"][0][9] == "index"


def test_capability_discovery_and_batch_ticker_requests(exchange_modules, gateway_config):
    rest, pro, _ = exchange_modules
    app = create_app(gateway_config, rest, pro)
    auth = headers(gateway_config)
    with TestClient(app) as client:
        capabilities = client.get("/v1/binance/capabilities", headers=auth)
        tickers = client.get(
            "/v1/binance/tickers",
            params={"symbols": "BTC/USDT,ETH/USDT"},
            headers=auth,
        )

    assert capabilities.status_code == 200
    assert capabilities.json()["marketTypes"] == ["contract", "margin", "spot", "swap"]
    assert capabilities.json()["rest"]["tickers"] == "supported"
    assert capabilities.json()["websocket"]["fundingRate"] == "supported"
    assert tickers.status_code == 200
    assert len(tickers.json()["tickers"]) == 2


def test_websocket_uses_one_upstream_subscription_for_clients(exchange_modules, gateway_config):
    rest, pro, metrics = exchange_modules
    app = create_app(gateway_config, rest, pro)
    subscription = {
        "op": "subscribe",
        "id": "ticker-1",
        "exchange": "binance",
        "channel": "ticker",
        "symbol": "BTC/USDT",
    }
    with TestClient(app) as client:
        with client.websocket_connect("/ws") as first, client.websocket_connect("/ws") as second:
            first.send_json({"op": "auth", "token": gateway_config.api_key})
            second.send_json({"op": "auth", "token": gateway_config.api_key})
            assert first.receive_json()["type"] == "ready"
            assert second.receive_json()["type"] == "ready"
            first.send_json(subscription)
            second.send_json({**subscription, "id": "ticker-2"})
            first_ack = first.receive_json()
            assert first_ack["type"] == "subscribed", first_ack
            second_ack = second.receive_json()
            assert second_ack["type"] == "subscribed", second_ack
            assert first.receive_json()["type"] == "data"
            assert second.receive_json()["type"] == "data"
            assert len(app.state.stream_hub.streams) == 1
        assert len(app.state.ws_clients) == 0
    assert metrics["closes"] >= 2


def test_websocket_exposes_contract_funding_stream(exchange_modules, gateway_config):
    rest, pro, _ = exchange_modules
    app = create_app(gateway_config, rest, pro)
    with TestClient(app) as client:
        with client.websocket_connect("/ws") as socket:
            socket.send_json({"op": "auth", "token": gateway_config.api_key})
            assert socket.receive_json()["type"] == "ready"
            socket.send_json(
                {
                    "op": "subscribe",
                    "id": "funding",
                    "exchange": "binance",
                    "channel": "fundingRate",
                    "symbol": "BTC/USDT:USDT",
                }
            )
            assert socket.receive_json()["type"] == "subscribed"
            update = socket.receive_json()
            assert update["channel"] == "fundingRate"
            assert update["data"]["fundingRate"] == 0.0001


def test_openapi_metrics_and_request_ids(exchange_modules, gateway_config):
    rest, pro, _ = exchange_modules
    app = create_app(gateway_config, rest, pro)
    with TestClient(app) as client:
        docs = client.get("/docs")
        spec = client.get("/openapi.json")
        denied = client.get("/metrics")
        metrics = client.get("/metrics", headers=headers(gateway_config))
        version = client.get("/version")

    assert docs.status_code == 200
    assert spec.status_code == 200
    assert "/v1/{exchange}/positions" in spec.json()["paths"]
    assert "HTTPBearer" in spec.json()["components"]["securitySchemes"]
    assert denied.status_code == 401
    assert metrics.status_code == 200
    assert "ccxt_gateway_http_requests_total" in metrics.text
    assert version.headers.get("x-request-id")


def test_account_read_routes_and_archived_market_data(exchange_modules, gateway_config):
    rest, pro, _ = exchange_modules

    class HistoryStore:
        enabled = True

        async def initialize(self):
            return None

        async def ping(self):
            return True

        async def save_candles(self, *args):
            self.candles = args

        async def save_trades(self, *args):
            self.trades = args

        async def query(
            self, kind, exchange, symbol, timeframe="", since=None, until=None, limit=1000
        ):
            return [{"kind": kind, "exchange": exchange, "symbol": symbol, "timeframe": timeframe}]

        async def audit(self, *args):
            return None

        async def close(self):
            return None

    history_store = HistoryStore()
    app = create_app(gateway_config, rest, pro, history_store_backend=history_store)
    auth = headers(gateway_config)
    with TestClient(app) as client:
        open_orders = client.get("/v1/binance/open-orders", headers=auth)
        order = client.get("/v1/binance/order/order-1", params={"symbol": "BTC/USDT"}, headers=auth)
        positions = client.get(
            "/v1/binance/positions", params={"symbols": "BTC/USDT:USDT"}, headers=auth
        )
        fee = client.get("/v1/binance/trading-fee", params={"symbol": "BTC/USDT"}, headers=auth)
        my_trades = client.get("/v1/binance/my-trades", params={"symbol": "BTC/USDT"}, headers=auth)
        candles = client.get(
            "/v1/binance/ohlcv",
            params={"symbol": "BTC/USDT", "timeframe": "1h"},
            headers=auth,
        )
        archived = client.get(
            "/v1/archive/ohlcv",
            params={
                "exchange": "binance",
                "symbol": "BTC/USDT",
                "timeframe": "1h",
            },
            headers=auth,
        )

    assert open_orders.json()["orders"][0]["id"] == "open-1"
    assert order.json()["order"]["id"] == "order-1"
    assert positions.json()["positions"][0]["contracts"] == 1
    assert fee.json()["fee"]["maker"] == 0.001
    assert my_trades.json()["trades"][0]["id"] == "trade-1"
    assert candles.status_code == 200
    assert history_store.candles[2:4] == ("1h", [[0, 1, 2, 3, 4, 5, "1h", 100, False, None]])
    assert archived.json()["source"] == "archive"


def test_orders_are_disabled_by_default_and_idempotent_when_enabled(
    exchange_modules, gateway_config
):
    rest, pro, metrics = exchange_modules
    payload = {
        "symbol": "BTC/USDT",
        "type": "limit",
        "side": "buy",
        "amount": 1,
        "price": 10,
    }
    app = create_app(gateway_config, rest, pro)
    with TestClient(app) as client:
        blocked = client.post(
            "/v1/binance/order",
            headers={**headers(gateway_config), "Idempotency-Key": "disabled-order-1"},
            json=payload,
        )
    assert blocked.status_code == 403
    assert blocked.json()["error"] == "trading_disabled"

    config = replace(gateway_config, trading_enabled=True)
    app = create_app(config, rest, pro)
    request_headers = {**headers(config), "Idempotency-Key": "limit-order-0001"}
    with TestClient(app) as client:
        first = client.post("/v1/binance/order", headers=request_headers, json=payload)
        replay = client.post("/v1/binance/order", headers=request_headers, json=payload)
        conflict = client.post(
            "/v1/binance/order",
            headers=request_headers,
            json={**payload, "amount": 2},
        )

    assert first.status_code == 201
    assert replay.status_code == 201
    assert conflict.status_code == 409
    assert metrics["order_calls"] == 1
