from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from .indicators import sma_slope, volume_ratio, vwap_from_bars


@dataclass
class ORB:
    """Opening Range Breakout levels for first 15 minutes by default."""

    high: float
    low: float
    ready: bool


@dataclass
class DailyLevels:
    """Previous Day High/Low/Close."""

    pdh: float
    pdl: float
    pdc: float


@dataclass
class Trend:
    """Trend label from SMA slope."""

    label: str  # "up" | "down" | "neutral"
    slope: float


@dataclass
class HL:
    """Intraday High/Low up to the latest completed bar in df_5m."""

    hod: float
    lod: float


@dataclass
class IndexContext:
    """
    Compact, deterministic pack the brain/graph consumes.
    All values derived from provided bars; no I/O here.
    """

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
    schema_version: str = "ctx_index_v1"


def _trend_from_slope(slope: float, eps: float = 1e-9) -> str:
    if slope > eps:
        return "up"
    if slope < -eps:
        return "down"
    return "neutral"


def get_orb(df_5m: pd.DataFrame, first_window: tuple[str, str] = ("09:15", "09:30")) -> ORB:
    """
    ORB computed from bars whose ts (datetime) falls within first_window (HH:MM strings).
    If window not complete yet, mark ready=False and seed with first bar extremas.
    Requires df_5m['ts'] is datetime64 and sorted ascending.
    """

    if df_5m.empty:
        return ORB(0.0, 0.0, False)
    ts_hm = df_5m["ts"].dt.strftime("%H:%M")
    mask = ts_hm.between(*first_window)
    if not mask.any():
        # window not reached yet; seed from first bar
        return ORB(
            high=float(df_5m["high"].iloc[0]),
            low=float(df_5m["low"].iloc[0]),
            ready=False,
        )
    window = df_5m.loc[mask]
    return ORB(high=float(window["high"].max()), low=float(window["low"].min()), ready=True)


def get_daily_levels(df_1d: pd.DataFrame) -> DailyLevels:
    """
    Uses the previous daily bar (df_1d sorted ascending).
    If not enough history, returns zeros.
    """

    if len(df_1d) < 2:
        return DailyLevels(0.0, 0.0, 0.0)
    prev = df_1d.iloc[-2]
    return DailyLevels(pdh=float(prev["high"]), pdl=float(prev["low"]), pdc=float(prev["close"]))


def get_intraday_highlow(df_5m: pd.DataFrame) -> HL:
    if df_5m.empty:
        return HL(0.0, 0.0)
    return HL(hod=float(df_5m["high"].max()), lod=float(df_5m["low"].min()))


def get_trend(df: pd.DataFrame, n: int) -> Trend:
    slope = float(sma_slope(df["close"], n))
    return Trend(label=_trend_from_slope(slope), slope=slope)


def build_ctx_index(symbol: str, df_1d: pd.DataFrame, df_15m: pd.DataFrame, df_5m: pd.DataFrame) -> IndexContext:
    """
    Build compact context from pre-fetched bars.
    Assumes:
      - df_1d, df_15m, df_5m are sorted ascending by 'ts'
      - Each has standard columns: ts, open, high, low, close, volume
    """

    if df_5m.empty:
        # minimal safe default
        return IndexContext(
            symbol=symbol,
            ts="",
            price=0.0,
            vwap=0.0,
            vwap_dist_pct=0.0,
            orb=ORB(0.0, 0.0, False),
            daily=DailyLevels(0.0, 0.0, 0.0),
            hod_lod=HL(0.0, 0.0),
            trend_15m=Trend("neutral", 0.0),
            trend_5m=Trend("neutral", 0.0),
            vol_ratio_5m=1.0,
            atr1d_pct=0.0,
        )

    # Price & timestamp
    price = float(df_5m["close"].iloc[-1])
    ts_str = str(df_5m["ts"].iloc[-1])

    # Core constructs
    vwap = vwap_from_bars(df_5m)
    vwap_dist_pct = ((price - vwap) / vwap * 100.0) if vwap else 0.0
    orb = get_orb(df_5m)
    daily = get_daily_levels(df_1d)
    hl = get_intraday_highlow(df_5m)
    trend15 = get_trend(df_15m, 10)
    trend5 = get_trend(df_5m, 10)
    volr = float(volume_ratio(df_5m["volume"], 20))

    # Simple ATR% proxy from daily ranges (mean 14)
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
        vwap_dist_pct=vwap_dist_pct,
        orb=orb,
        daily=daily,
        hod_lod=hl,
        trend_15m=trend15,
        trend_5m=trend5,
        vol_ratio_5m=volr,
        atr1d_pct=atr_pct,
    )
