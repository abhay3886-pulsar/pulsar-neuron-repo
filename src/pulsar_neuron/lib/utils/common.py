from __future__ import annotations

LOT_SIZES = {"NIFTY": 50, "BANKNIFTY": 15}
TICK_SIZE = 0.05


def lot_size(symbol: str) -> int:
    return LOT_SIZES.get(symbol, 1)


def round_to_tick(price: float) -> float:
    return round(price / TICK_SIZE) * TICK_SIZE
