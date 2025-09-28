from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from .indicators import sma_slope, swing_points, volume_ratio, vwap_distance


@dataclass
class IndexContext:
    symbol: str
    ts: str
    price: float
    vwap: float | None
    vwap_dist_pct: float
    sma10_15m_slope: float
    sma10_5m_slope: float
    vol_ratio_5m: float
    swing_hi_5m: float | None
    swing_lo_5m: float | None
    schema_version: str = "ctx_index_v1"


def build_ctx_index(
    symbol: str, df_5m: pd.DataFrame, df_15m: pd.DataFrame, vwap: float | None
) -> IndexContext:
    price = float(df_5m["close"].iloc[-1])
    swing_hi, swing_lo = swing_points(df_5m, 20)
    return IndexContext(
        symbol=symbol,
        ts=str(df_5m["ts"].iloc[-1]),
        price=price,
        vwap=vwap,
        vwap_dist_pct=vwap_distance(price, vwap) if vwap else 0.0,
        sma10_15m_slope=sma_slope(df_15m["close"], 10),
        sma10_5m_slope=sma_slope(df_5m["close"], 10),
        vol_ratio_5m=volume_ratio(df_5m["volume"], 20),
        swing_hi_5m=swing_hi,
        swing_lo_5m=swing_lo,
    )
