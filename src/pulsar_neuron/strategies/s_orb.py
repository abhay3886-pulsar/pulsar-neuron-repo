from __future__ import annotations

from typing import Optional


THRESHOLD_PCT = 0.3


def run(ctx: dict) -> Optional[str]:
    """Open-range breakout directional bias."""

    closes = ctx.get("closes5")
    if not closes or len(closes) < 2:
        return None

    open_val = closes[0]
    last_val = closes[-1]
    if not open_val:
        return None

    change_pct = ((last_val - open_val) / open_val) * 100
    if change_pct > THRESHOLD_PCT:
        return "bullish"
    if change_pct < -THRESHOLD_PCT:
        return "bearish"
    return None
