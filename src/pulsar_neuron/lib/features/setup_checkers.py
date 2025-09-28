from __future__ import annotations

"""Deterministic setup checkers working purely off :class:`IndexContext`."""

from typing import Dict

from .context import IndexContext


def vwap_band_status(vwap_dist_pct: float, band: float = 0.6) -> Dict[str, float | bool]:
    """Return whether price is within the configurable VWAP band."""

    inside = abs(float(vwap_dist_pct)) <= band
    return {"inside": inside, "dist_pct": float(vwap_dist_pct)}


def trend_alignment(ctx: IndexContext) -> Dict[str, str | bool]:
    """Check if the 15m and 5m trends are aligned and non-neutral."""

    aligned = ctx.trend_15m.label == ctx.trend_5m.label != "neutral"
    direction = ctx.trend_15m.label if aligned else "neutral"
    return {"aligned": aligned, "dir": direction}


def is_orb_breakout(ctx: IndexContext, side: str, vol_thresh: float = 1.2, band: float = 0.6) -> bool:
    """Return ``True`` when ORB breakout conditions are satisfied for ``side``."""

    if not ctx.orb.ready:
        return False
    align = trend_alignment(ctx)
    band_ok = vwap_band_status(ctx.vwap_dist_pct, band)["inside"]
    if side.lower() == "long":
        return (
            align["aligned"]
            and align["dir"] == "up"
            and band_ok
            and ctx.price > ctx.orb.high
            and ctx.vol_ratio_5m > vol_thresh
        )
    if side.lower() == "short":
        return (
            align["aligned"]
            and align["dir"] == "down"
            and band_ok
            and ctx.price < ctx.orb.low
            and ctx.vol_ratio_5m > vol_thresh
        )
    return False


def is_vwap_retest(ctx: IndexContext, side: str, band: float = 0.6) -> bool:
    """Return ``True`` if price respects VWAP band with aligned trend."""

    align = trend_alignment(ctx)
    band_ok = vwap_band_status(ctx.vwap_dist_pct, band)["inside"]
    if side.lower() == "long":
        return align["aligned"] and align["dir"] == "up" and band_ok and ctx.price >= ctx.vwap
    if side.lower() == "short":
        return align["aligned"] and align["dir"] == "down" and band_ok and ctx.price <= ctx.vwap
    return False


def is_cpr_break(ctx: IndexContext, side: str, vol_thresh: float = 1.1) -> bool:
    """Return ``True`` when price breaks CPR in direction of aligned trend."""

    align = trend_alignment(ctx)
    if side.lower() == "long":
        return (
            align["aligned"]
            and align["dir"] == "up"
            and ctx.price > ctx.cpr.tc
            and ctx.vol_ratio_5m > vol_thresh
        )
    if side.lower() == "short":
        return (
            align["aligned"]
            and align["dir"] == "down"
            and ctx.price < ctx.cpr.bc
            and ctx.vol_ratio_5m > vol_thresh
        )
    return False
