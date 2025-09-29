from __future__ import annotations

from datetime import datetime, timedelta

import pandas as pd
import pytest

from pulsar_neuron.lib.features.indicators import (
    average_true_range,
    distance_from_vwap_pct,
    ema,
    rsi,
    true_range,
    vwap_from_bars,
)


def _synthetic_ohlcv(rows: int) -> pd.DataFrame:
    base = datetime(2024, 1, 1)
    data_rows = []
    for i in range(rows):
        data_rows.append(
            {
                "ts": base + timedelta(minutes=i),
                "open": 100.0 + float(i),
                "high": 101.0 + float(i),
                "low": 99.0 + float(i),
                "close": 100.5 + float(i),
                "volume": 1000.0 + float(10 * i),
            }
        )
    return pd.DataFrame(data_rows)


def _assert_series_values(series: pd.Series, expected: list[float | None]) -> None:
    actual = series.to_list()
    assert len(actual) == len(expected)
    for got, exp in zip(actual, expected):
        if exp is None:
            assert got is None
        else:
            assert got == pytest.approx(exp, abs=1e-6)


def test_ema_matches_manual_computation() -> None:
    series = pd.Series([1.0, 2.0, 3.0, 4.0, 5.0])
    result = ema(series, span=3)
    expected = [None, None, 2.0, 3.0, 4.0]
    _assert_series_values(result, expected)


@pytest.mark.parametrize(
    "prices, expected",
    [
        ([1.0] * 10, 50.0),
        ([float(i) for i in range(1, 11)], 100.0),
        ([float(i) for i in range(10, 0, -1)], 0.0),
    ],
)
def test_rsi_edge_cases(prices: list[float], expected: float) -> None:
    series = pd.Series(prices)
    result = rsi(series, n=3)
    assert pytest.approx(result.to_list()[-1], abs=1e-6) == expected


def test_true_range_and_average_true_range() -> None:
    base = datetime(2024, 1, 1)
    rows = [
        {"ts": base, "open": 9.0, "high": 11.0, "low": 9.0, "close": 10.0, "volume": 1000.0},
        {
            "ts": base + timedelta(minutes=1),
            "open": 10.0,
            "high": 12.0,
            "low": 10.0,
            "close": 11.0,
            "volume": 1010.0,
        },
        {
            "ts": base + timedelta(minutes=2),
            "open": 11.0,
            "high": 13.0,
            "low": 11.0,
            "close": 12.0,
            "volume": 1020.0,
        },
        {
            "ts": base + timedelta(minutes=3),
            "open": 12.0,
            "high": 13.5,
            "low": 11.5,
            "close": 12.5,
            "volume": 1030.0,
        },
        {
            "ts": base + timedelta(minutes=4),
            "open": 12.5,
            "high": 16.5,
            "low": 12.5,
            "close": 13.5,
            "volume": 1040.0,
        },
    ]
    df = pd.DataFrame(rows)

    tr = true_range(df)
    assert tr.to_list() == [2.0, 2.0, 2.0, 2.0, 4.0]

    atr = average_true_range(df, 3)
    expected_atr = [None, None, 2.0, 2.0, 8.0 / 3.0]
    _assert_series_values(atr, expected_atr)


def test_vwap_helpers_consistency() -> None:
    df = _synthetic_ohlcv(3)
    vwap = vwap_from_bars(df)
    price = df["close"].iloc[-1]
    dist = distance_from_vwap_pct(price, vwap)

    assert vwap > 0
    assert dist == pytest.approx((price - vwap) / vwap * 100.0)
