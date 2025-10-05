from __future__ import annotations

from datetime import datetime, timezone
from typing import Mapping


def normalize_option_row(row: Mapping) -> dict:
    """Normalize option chain row into DB schema."""

    required = ("symbol", "strike", "side", "ltp")
    for key in required:
        if row.get(key) is None:
            raise KeyError(f"options row missing '{key}'")

    ts = row.get("ts_ist")
    if ts is None:
        ts = datetime.now(timezone.utc)

    return {
        "symbol": str(row["symbol"]),
        "ts_ist": ts,
        "expiry": row.get("expiry"),
        "strike": float(row["strike"]),
        "side": str(row["side"]),
        "ltp": float(row["ltp"]),
        "iv": float(row.get("iv", 0.0)),
        "oi": int(row.get("oi", 0)),
        "volume": int(row.get("volume", 0)),
        "delta": float(row.get("delta", 0.0)),
        "gamma": float(row.get("gamma", 0.0)),
        "theta": float(row.get("theta", 0.0)),
        "vega": float(row.get("vega", 0.0)),
    }
