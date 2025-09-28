from __future__ import annotations

from ..features.ctx_index import IndexContext


def scan_long_setup(ctx: IndexContext) -> dict | None:
    if ctx.sma10_15m_slope > 0 and abs(ctx.vwap_dist_pct) <= 0.6 and ctx.vol_ratio_5m > 1.2:
        level = ctx.swing_hi_5m or ctx.price
        return {
            "symbol": ctx.symbol,
            "side": "BUY",
            "entry_level": level,
            "reason": "15m_up VWAP band vol>1.2",
        }
    return None


def scan_short_setup(ctx: IndexContext) -> dict | None:
    if ctx.sma10_15m_slope < 0 and abs(ctx.vwap_dist_pct) <= 0.6 and ctx.vol_ratio_5m > 1.2:
        level = ctx.swing_lo_5m or ctx.price
        return {
            "symbol": ctx.symbol,
            "side": "SELL",
            "entry_level": level,
            "reason": "15m_down VWAP band vol>1.2",
        }
    return None


def initial_sl(entry: float, ctx: IndexContext, side: str) -> float:
    ref = ctx.swing_lo_5m if side == "BUY" else ctx.swing_hi_5m
    if ref is None:
        return entry * (0.997 if side == "BUY" else 1.003)
    return float(ref)
