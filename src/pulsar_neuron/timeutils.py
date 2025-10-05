"""Time helpers with IST-aware utilities."""

from __future__ import annotations

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from pulsar_neuron.config.loader import load_defaults
from pulsar_neuron.providers.market_provider import Timeframe

_MARKET_CFG = load_defaults().get("market", {})
_TZ_NAME = _MARKET_CFG.get("tz", "Asia/Kolkata")
_IST = ZoneInfo(_TZ_NAME)


def now_ist() -> datetime:
    """Return the current wall-clock time in IST."""

    return datetime.now(tz=_IST)


def _align_daily_cutoff(ts: datetime) -> datetime:
    return ts.replace(hour=15, minute=30, second=0, microsecond=0)


def is_bar_complete(ts_ist: datetime, tf: Timeframe, grace_s: int) -> bool:
    """Check if the bar ending at ``ts_ist`` is complete for ``tf``."""

    if ts_ist.tzinfo is None:
        ts_ist = ts_ist.replace(tzinfo=_IST)
    else:
        ts_ist = ts_ist.astimezone(_IST)

    if now_ist() < ts_ist + timedelta(seconds=grace_s):
        return False

    if tf == "1d":
        return ts_ist >= _align_daily_cutoff(ts_ist)

    if ts_ist.second != 0 or ts_ist.microsecond != 0:
        return False

    minute = ts_ist.minute
    if tf == "5m":
        return minute % 5 == 0
    if tf == "15m":
        return minute % 15 == 0

    raise ValueError(f"Unsupported timeframe: {tf}")


__all__ = ["now_ist", "is_bar_complete"]
