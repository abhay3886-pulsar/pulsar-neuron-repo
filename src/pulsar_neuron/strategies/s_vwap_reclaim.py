from __future__ import annotations


def run(ctx: dict) -> str | None:
    """Simple VWAP reclaim heuristic based on slope sign."""

    slope = ctx.get("slope_5m")
    if slope is None:
        return None
    return "bullish" if slope > 0 else "bearish"
