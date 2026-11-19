import pytest

from app.config import GatewayConfig


def test_config_requires_long_gateway_key():
    with pytest.raises(ValueError, match="API_KEY"):
        GatewayConfig.from_env({"API_KEY": "short"})


def test_config_parses_credentials_and_limits():
    config = GatewayConfig.from_env(
        {
            "API_KEY": "x" * 32,
            "EXCHANGE_CREDENTIALS_JSON": '{"binance":{"apiKey":"a","secret":"s"}}',
            "RATE_LIMIT_REQUESTS": "7",
        }
    )
    assert config.exchange_credentials["binance"]["apiKey"] == "a"
    assert config.rate_limit_requests == 7


@pytest.mark.parametrize("raw", ["[]", "null", "not-json"])
def test_config_rejects_invalid_credential_json(raw):
    with pytest.raises(ValueError):
        GatewayConfig.from_env({"API_KEY": "x" * 32, "EXCHANGE_CREDENTIALS_JSON": raw})
