def test_ccxt_async_and_pro_expose_contract_market_methods():
    import ccxt.async_support as ccxt_async
    import ccxt.pro as ccxt_pro

    assert "binance" in ccxt_async.exchanges
    for method in (
        "fetch_funding_rate",
        "fetch_funding_rate_history",
        "fetch_open_interest",
        "fetch_open_interest_history",
        "fetch_mark_price",
    ):
        assert callable(getattr(ccxt_async.binance, method, None))

    for method in ("watch_funding_rate", "watch_mark_price"):
        assert callable(getattr(ccxt_pro.binance, method, None))
