from __future__ import annotations


def run(ctx: dict) -> str | None:
    """Trend continuation bias from SMA and slope."""

    sma = ctx.get("sma20_5m")
    slope = ctx.get("slope_5m")
    if sma is None or slope is None:
        return None

    if slope > 0:
        return "bullish"
    if slope < 0:
        return "bearish"
    return None
