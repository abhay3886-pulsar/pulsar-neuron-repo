from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

LOT_SIZES = {"NIFTY": 50, "BANKNIFTY": 15}
TICK_SIZE = 0.05


def lot_size(symbol: str) -> int:
    """Return lot size for supported index symbols."""

    return LOT_SIZES.get(symbol.upper(), 1)


def round_to_tick(price: float) -> float:
    """Round to the nearest exchange tick size with deterministic bias."""

    value = Decimal(str(price))
    tick = Decimal(str(TICK_SIZE))
    remainder = value % tick
    threshold = tick * Decimal("0.6")
    if remainder <= threshold:
        result = value - remainder
    else:
        result = value - remainder + tick
    return float(result)


@dataclass(frozen=True)
class Mode:
    DRY_RUN: str = "DRY_RUN"
    PAPER: str = "PAPER"
    LIVE: str = "LIVE"
