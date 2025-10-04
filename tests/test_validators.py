from datetime import datetime

import pytest

from pulsar_neuron.lib.timeutils import ist_tz
from pulsar_neuron.lib.validators import (
    enforce_bar_complete,
    ensure_sorted_unique,
    validate_ohlcv_row,
)


def _base_row(**overrides):
    tz = ist_tz()
    row = {
        "symbol": "TEST",
        "ts_ist": datetime(2024, 1, 1, 9, 30, tzinfo=tz),
        "tf": "15m",
        "o": 100.0,
        "h": 110.0,
        "l": 95.0,
        "c": 105.0,
        "v": 10,
    }
    row.update(overrides)
    return row


def test_validate_row_success():
    validate_ohlcv_row(_base_row())


def test_validate_row_invalid_prices():
    with pytest.raises(ValueError):
        validate_ohlcv_row(_base_row(l=120))


def test_validate_row_naive_timestamp():
    with pytest.raises(ValueError):
        validate_ohlcv_row(_base_row(ts_ist=datetime(2024, 1, 1, 9, 30)))


def test_validate_row_non_boundary():
    tz = ist_tz()
    with pytest.raises(ValueError):
        validate_ohlcv_row(_base_row(ts_ist=datetime(2024, 1, 1, 9, 32, tzinfo=tz)))


def test_enforce_bar_complete():
    enforce_bar_complete(_base_row())
    with pytest.raises(ValueError):
        enforce_bar_complete(_base_row(ts_ist=datetime(2024, 1, 1, 9, 15, tzinfo=ist_tz())))


def test_ensure_sorted_unique_duplicate():
    tz = ist_tz()
    rows = [
        _base_row(ts_ist=datetime(2024, 1, 1, 9, 30, tzinfo=tz)),
        _base_row(ts_ist=datetime(2024, 1, 1, 9, 30, tzinfo=tz)),
    ]
    with pytest.raises(ValueError):
        ensure_sorted_unique(rows)

