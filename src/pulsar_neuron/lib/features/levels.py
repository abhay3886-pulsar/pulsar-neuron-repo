from __future__ import annotations

"""Session level helpers used by the intraday context builder."""

from dataclasses import dataclass

import pandas as pd


@dataclass
class DailyLevels:
    """Previous-day high, low and close levels."""

    pdh: float
    pdl: float
    pdc: float


@dataclass
class ORB:
    """Opening range breakout high/low and readiness flag."""

    high: float
    low: float
    ready: bool


@dataclass
class CPR:
    """Central Pivot Range consisting of pivot, bottom and top central levels."""

    pivot: float
    bc: float
    tc: float


@dataclass
class HL:
    """Intraday high and low computed from the 5-minute dataframe."""

    hod: float
    lod: float


def get_daily_levels(df_1d: pd.DataFrame) -> DailyLevels:
    """Return previous day high/low/close levels from a daily dataframe."""

    if len(df_1d) < 2:
        return DailyLevels(0.0, 0.0, 0.0)
    prev = df_1d.iloc[-2]
    return DailyLevels(pdh=float(prev["high"]), pdl=float(prev["low"]), pdc=float(prev["close"]))


def get_orb(df_5m: pd.DataFrame, window: tuple[str, str] = ("09:15", "09:30")) -> ORB:
    """Compute the opening range breakout levels within the provided window."""

    if df_5m.empty:
        return ORB(0.0, 0.0, False)
    ts_hm = df_5m["ts"].dt.strftime("%H:%M")
    mask = ts_hm.between(*window)
    if not mask.any():
        return ORB(float(df_5m["high"].iloc[0]), float(df_5m["low"].iloc[0]), False)
    window_df = df_5m.loc[mask]
    return ORB(float(window_df["high"].max()), float(window_df["low"].min()), True)


def get_cpr(df_1d: pd.DataFrame) -> CPR:
    """Compute the Central Pivot Range from the previous daily bar."""

    if len(df_1d) < 2:
        return CPR(0.0, 0.0, 0.0)
    prev = df_1d.iloc[-2]
    pivot = (prev["high"] + prev["low"] + prev["close"]) / 3.0
    bc = (prev["high"] + prev["low"]) / 2.0
    tc = 2 * pivot - bc
    return CPR(pivot=float(pivot), bc=float(bc), tc=float(tc))


def get_intraday_highlow(df_5m: pd.DataFrame) -> HL:
    """Return the highest and lowest traded prices in the session dataframe."""

    if df_5m.empty:
        return HL(0.0, 0.0)
    return HL(float(df_5m["high"].max()), float(df_5m["low"].min()))

