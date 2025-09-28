from datetime import datetime

from pulsar_neuron.lib.data.feeds import get_last_n, get_live_quote, get_ohlcv


def test_get_ohlcv_deterministic():
    start = datetime(2025, 1, 1, 9, 15)
    end = datetime(2025, 1, 1, 9, 45)
    bars1 = get_ohlcv("NIFTY", "5m", start, end)
    bars2 = get_ohlcv("NIFTY", "5m", start, end)
    assert len(bars1) == len(bars2)
    assert bars1.iloc[0]["ts"] == bars2.iloc[0]["ts"]
    assert bars1.iloc[-1]["close"] == bars2.iloc[-1]["close"]


def test_get_last_n_shape_and_quote_consistency():
    bars = get_last_n("BANKNIFTY", "15m", 3)
    assert len(bars) == 3
    assert bars.iloc[0]["open"] != 0
    quote = get_live_quote("BANKNIFTY")
    assert quote["bid"] < quote["ask"]
    assert quote["ltp"] == quote["bid"] + 0.05


def test_get_last_n_zero_returns_empty():
    empty = get_last_n("TEST", "1d", 0)
    assert empty.empty

