from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from .indicators import sma_slope, volume_ratio, vwap_from_bars


@dataclass
class ORB:
    high: float
    low: float
    ready: bool


@dataclass
class DailyLevels:
    pdh: float
    pdl: float
    pdc: float


@dataclass
class Trend:
    label: str
    slope: float


@dataclass
class HL:
    hod: float
    lod: float


@dataclass
class IndexContext:
    symbol: str
    ts: str
    price: float
    vwap: float
    vwap_dist_pct: float
    orb: ORB
    daily: DailyLevels
    hod_lod: HL
    trend_15m: Trend
    trend_5m: Trend
    vol_ratio_5m: float
    atr1d_pct: float


def _trend_from_slope(slope: float, eps: float = 1e-9) -> str:
    if slope > eps:
        return "up"
    if slope < -eps:
        return "down"
    return "neutral"


def get_orb(df_5m: pd.DataFrame, first_window: tuple[str, str] = ("09:15", "09:30")) -> ORB:
    mask = df_5m["ts"].dt.strftime("%H:%M").between(*first_window)
    if not mask.any():
        return ORB(
            high=float(df_5m["high"].iloc[:1].max()) if not df_5m.empty else 0.0,
            low=float(df_5m["low"].iloc[:1].min()) if not df_5m.empty else 0.0,
            ready=False,
        )
    window = df_5m.loc[mask]
    return ORB(high=float(window["high"].max()), low=float(window["low"].min()), ready=True)


def get_daily_levels(df_1d: pd.DataFrame) -> DailyLevels:
    """Use previous day row to compute PDH/PDL/PDC. df_1d sorted ascending."""

    if len(df_1d) < 2:
        return DailyLevels(0.0, 0.0, 0.0)
    prev = df_1d.iloc[-2]
    return DailyLevels(pdh=float(prev["high"]), pdl=float(prev["low"]), pdc=float(prev["close"]))


def get_intraday_highlow(df_5m: pd.DataFrame) -> HL:
    return HL(
        hod=float(df_5m["high"].max() if not df_5m.empty else 0.0),
        lod=float(df_5m["low"].min() if not df_5m.empty else 0.0),
    )


def get_trend(df: pd.DataFrame, n: int) -> Trend:
    slope = sma_slope(df["close"], n)
    return Trend(label=_trend_from_slope(slope), slope=float(slope))


def build_ctx_index(symbol: str, df_1d: pd.DataFrame, df_15m: pd.DataFrame, df_5m: pd.DataFrame) -> IndexContext:
    """Build compact context from pre-fetched bars (no I/O)."""

    price = float(df_5m["close"].iloc[-1])
    vwap = vwap_from_bars(df_5m)
    vwap_dist_pct = ((price - vwap) / vwap * 100.0) if vwap else 0.0
    orb = get_orb(df_5m)
    daily = get_daily_levels(df_1d)
    hl = get_intraday_highlow(df_5m)
    trend15 = get_trend(df_15m, 10)
    trend5 = get_trend(df_5m, 10)
    volr = volume_ratio(df_5m["volume"], 20)
    if len(df_1d) >= 15:
        rng = (df_1d["high"] - df_1d["low"]).rolling(14, min_periods=14).mean().iloc[-1]
        atr_pct = float((rng / df_1d["close"].iloc[-1]) * 100.0)
    else:
        atr_pct = 0.0
    return IndexContext(
        symbol=symbol,
        ts=str(df_5m["ts"].iloc[-1]),
        price=price,
        vwap=vwap,
        vwap_dist_pct=vwap_dist_pct,
        orb=orb,
        daily=daily,
        hod_lod=hl,
        trend_15m=trend15,
        trend_5m=trend5,
        vol_ratio_5m=float(volr),
        atr1d_pct=atr_pct,
    )
