from __future__ import annotations

from typing import Literal

import pandas as pd

Timeframe = Literal["1d", "4h", "15m", "5m"]


def get_ohlcv(symbol: str, tf: Timeframe, start, end) -> pd.DataFrame:
    """Return OHLCV DataFrame with ['ts','open','high','low','close','volume'] (stub)."""
    raise NotImplementedError


def get_last_n(symbol: str, tf: Timeframe, n: int) -> pd.DataFrame:
    """Fast path to fetch last N bars (stub)."""
    raise NotImplementedError
