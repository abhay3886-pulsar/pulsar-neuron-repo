from __future__ import annotations

"""Index context builder used by trading brains for NIFTY/BANKNIFTY."""

from dataclasses import dataclass

import pandas as pd

from .indicators import distance_from_vwap_pct, sma_slope, volume_ratio, vwap_from_bars
from .levels import CPR, HL, ORB, DailyLevels, get_cpr, get_daily_levels, get_intraday_highlow, get_orb


@dataclass
class Trend:
    """Trend label and SMA slope expressed in absolute price units."""

    label: str
    slope: float


@dataclass
class IndexContext:
    """Deterministic snapshot of session state for an index symbol."""

    symbol: str
    ts: str
    price: float
    vwap: float
    vwap_dist_pct: float
    orb: ORB
    cpr: CPR
    daily: DailyLevels
    hod_lod: HL
    trend_15m: Trend
    trend_5m: Trend
    vol_ratio_5m: float
    atr1d_pct: float
    schema_version: str = "ctx_index_v2"


def _trend_from_series(series: pd.Series, n: int) -> Trend:
    """Return the ``Trend`` representation for the provided closing series."""

    slope = float(sma_slope(series, n))
    if slope > 0:
        label = "up"
    elif slope < 0:
        label = "down"
    else:
        label = "neutral"
    return Trend(label=label, slope=slope)


def build_ctx_index(symbol: str, df_1d: pd.DataFrame, df_15m: pd.DataFrame, df_5m: pd.DataFrame) -> IndexContext:
    """Build an :class:`IndexContext` using deterministic computations.

    All dataframes must be sorted in ascending order and contain columns
    ``ts``, ``open``, ``high``, ``low``, ``close`` and ``volume``.
    """

    if df_5m.empty:
        return IndexContext(
            symbol=symbol,
            ts="",
            price=0.0,
            vwap=0.0,
            vwap_dist_pct=0.0,
            orb=ORB(0.0, 0.0, False),
            cpr=CPR(0.0, 0.0, 0.0),
            daily=DailyLevels(0.0, 0.0, 0.0),
            hod_lod=HL(0.0, 0.0),
            trend_15m=Trend("neutral", 0.0),
            trend_5m=Trend("neutral", 0.0),
            vol_ratio_5m=1.0,
            atr1d_pct=0.0,
        )

    price = float(df_5m["close"].iloc[-1])
    ts_str = str(df_5m["ts"].iloc[-1])
    vwap = vwap_from_bars(df_5m)
    orb = get_orb(df_5m)
    cpr = get_cpr(df_1d)
    daily = get_daily_levels(df_1d)
    hl = get_intraday_highlow(df_5m)
    trend15 = _trend_from_series(df_15m["close"], 10) if not df_15m.empty else Trend("neutral", 0.0)
    trend5 = _trend_from_series(df_5m["close"], 10)
    volr = volume_ratio(df_5m["volume"], 20)

    if len(df_1d) >= 15:
        atr_points = (df_1d["high"] - df_1d["low"]).rolling(14, min_periods=14).mean().iloc[-1]
        atr_pct = float((atr_points / df_1d["close"].iloc[-1]) * 100.0)
    else:
        atr_pct = 0.0

    return IndexContext(
        symbol=symbol,
        ts=ts_str,
        price=price,
        vwap=vwap,
        vwap_dist_pct=distance_from_vwap_pct(price, vwap),
        orb=orb,
        cpr=cpr,
        daily=daily,
        hod_lod=hl,
        trend_15m=trend15,
        trend_5m=trend5,
        vol_ratio_5m=volr,
        atr1d_pct=atr_pct,
        schema_version="ctx_index_v2",
    )

