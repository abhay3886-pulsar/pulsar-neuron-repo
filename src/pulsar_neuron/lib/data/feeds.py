from __future__ import annotations

from typing import Literal

import pandas as pd

Timeframe = Literal["1d", "15m", "5m"]


def get_ohlcv(symbol: str, tf: Timeframe, start, end) -> pd.DataFrame:
    """Stub: implement against your DB/API later."""

    raise NotImplementedError


def get_last_n(symbol: str, tf: Timeframe, n: int) -> pd.DataFrame:
    """Stub: implement a fast path later."""

    raise NotImplementedError
