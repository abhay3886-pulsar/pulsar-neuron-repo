from __future__ import annotations

from datetime import datetime, timezone
from typing import Mapping


def normalize_fut_oi(row: Mapping) -> dict:
    """Normalize raw futures OI payload into the DB schema."""

    symbol = row.get("symbol")
    if symbol is None:
        raise KeyError("fut_oi row missing 'symbol'")

    ts = row.get("ts_ist")
    if ts is None:
        ts = datetime.now(timezone.utc)

    return {
        "symbol": str(symbol),
        "ts_ist": ts,
        "price": float(row.get("price", 0.0)),
        "oi": int(row.get("oi", 0)),
        "tag": row.get("tag"),
    }
