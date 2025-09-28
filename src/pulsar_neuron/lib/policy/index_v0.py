from __future__ import annotations

from ..features.ctx_index import IndexContext


def scan_long_setup(ctx: IndexContext) -> dict | None:
    if ctx.trend_15m.label == "up" and abs(ctx.vwap_dist_pct) <= 0.6 and ctx.vol_ratio_5m > 1.2:
        level = max(ctx.orb.high, ctx.price) if ctx.orb.ready else ctx.price
        return {
            "symbol": ctx.symbol,
            "side": "BUY",
            "entry_level": level,
            "reason": "15m_up VWAP_band vol>1.2",
        }
    return None


def scan_short_setup(ctx: IndexContext) -> dict | None:
    if ctx.trend_15m.label == "down" and abs(ctx.vwap_dist_pct) <= 0.6 and ctx.vol_ratio_5m > 1.2:
        level = min(ctx.orb.low, ctx.price) if ctx.orb.ready else ctx.price
        return {
            "symbol": ctx.symbol,
            "side": "SELL",
            "entry_level": level,
            "reason": "15m_down VWAP_band vol>1.2",
        }
    return None


def initial_sl(entry: float, ctx: IndexContext, side: str) -> float:
    if ctx.orb.ready:
        return ctx.orb.low if side == "BUY" else ctx.orb.high
    dist = entry * 0.0035
    return entry - dist if side == "BUY" else entry + dist
