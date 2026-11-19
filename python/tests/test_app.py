from dataclasses import replace

from fastapi.testclient import TestClient

from app.main import create_app


def headers(config):
    return {"Authorization": f"Bearer {config.api_key}"}


def test_rest_auth_cache_private_guard_and_order(exchange_modules, gateway_config):
    rest, pro, metrics = exchange_modules
    app = create_app(gateway_config, rest, pro)
    with TestClient(app) as client:
        assert client.get("/health/ready").json() == {"status": "ready"}
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
            headers=headers(gateway_config),
            json={"symbol": "BTC/USDT", "type": "limit", "side": "buy", "amount": 1, "price": 10},
        )
        assert placed.status_code == 201
        assert metrics["order_calls"] == 1
        assert (
            client.post(
                "/v1/binance/order",
                headers=headers(gateway_config),
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
