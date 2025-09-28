from __future__ import annotations

from typing import Literal

import pandas as pd

Timeframe = Literal["1d", "15m", "5m"]


def get_ohlcv(symbol: str, tf: Timeframe, start, end) -> pd.DataFrame:
    """Stub: to be implemented against your store/Broker later."""

    raise NotImplementedError("Wire your data source here.")


def get_last_n(symbol: str, tf: Timeframe, n: int) -> pd.DataFrame:
    """Stub fast path."""

    raise NotImplementedError("Wire your cache/store here.")
