"""Time and session utilities for IST-aware trading bars."""

from __future__ import annotations

import logging
from datetime import date, datetime, time, timedelta, timezone
from typing import Literal
from zoneinfo import ZoneInfo

logger = logging.getLogger(__name__)

Timeframe = Literal["5m", "15m", "1d"]

_IST_ZONE = ZoneInfo("Asia/Kolkata")
_SESSION_START = time(9, 15)
_SESSION_END = time(15, 30)
_TF_TO_MINUTES = {"5m": 5, "15m": 15, "1d": 1440}


def ist_tz() -> ZoneInfo:
    """Return the reusable IST timezone instance."""

    return _IST_ZONE


def _ensure_tzaware(dt: datetime) -> None:
    if dt.tzinfo is None:
        raise ValueError("Datetime must be timezone-aware")


def to_ist(dt: datetime) -> datetime:
    """Convert ``dt`` to IST, treating naive input as UTC."""

    if dt.tzinfo is None:
        logger.debug("Converting naive datetime to IST by assuming UTC: %s", dt)
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(_IST_ZONE)


def session_bounds(d: date) -> tuple[datetime, datetime]:
    """Return the start and end of the trading session in IST for ``d``."""

    start = datetime.combine(d, _SESSION_START, tzinfo=_IST_ZONE)
    end = datetime.combine(d, _SESSION_END, tzinfo=_IST_ZONE)
    return start, end


def tf_minutes(tf: Timeframe) -> int:
    """Return timeframe length in minutes."""

    try:
        return _TF_TO_MINUTES[tf]
    except KeyError as exc:  # pragma: no cover - defensive
        raise ValueError(f"Unsupported timeframe: {tf!r}") from exc


def is_intraday(tf: Timeframe) -> bool:
    """Return ``True`` for intraday timeframes."""

    return tf in ("5m", "15m")


def _require_ist(ts: datetime) -> datetime:
    _ensure_tzaware(ts)
    return to_ist(ts)


def is_bar_boundary(ts: datetime, tf: Timeframe) -> bool:
    """Return ``True`` if ``ts`` is a valid bar boundary for ``tf``."""

    ts_ist = _require_ist(ts)
    if ts_ist.second != 0 or ts_ist.microsecond != 0:
        return False

    if tf == "1d":
        _, session_end = session_bounds(ts_ist.date())
        return ts_ist == session_end

    start, end = session_bounds(ts_ist.date())
    if ts_ist <= start or ts_ist > end:
        return False
    delta = ts_ist - start
    minutes = int(delta.total_seconds() // 60)
    interval = tf_minutes(tf)
    return minutes % interval == 0


def floor_to_tf(ts: datetime, tf: Timeframe) -> datetime:
    """Floor ``ts`` to the most recent bar boundary for ``tf`` in IST."""

    ts_ist = _require_ist(ts)

    if tf == "1d":
        start, end = session_bounds(ts_ist.date())
        if ts_ist >= start:
            return end
        previous_day = ts_ist.date() - timedelta(days=1)
        return session_bounds(previous_day)[1]

    start, end = session_bounds(ts_ist.date())
    if ts_ist < start:
        previous_day = ts_ist.date() - timedelta(days=1)
        return session_bounds(previous_day)[1]
    if ts_ist >= end:
        return end

    delta = ts_ist - start
    minutes = int(delta.total_seconds() // 60)
    interval = tf_minutes(tf)
    floored_minutes = (minutes // interval) * interval
    return start + timedelta(minutes=floored_minutes)


def next_bar_end(after: datetime, tf: Timeframe) -> datetime:
    """Return the next bar boundary strictly greater than ``after``."""

    ts_ist = _require_ist(after)

    if tf == "1d":
        _, end = session_bounds(ts_ist.date())
        if ts_ist < end:
            return end
        next_day = ts_ist.date() + timedelta(days=1)
        return session_bounds(next_day)[1]

    interval = tf_minutes(tf)
    start, end = session_bounds(ts_ist.date())
    if ts_ist >= end:
        next_day = ts_ist.date() + timedelta(days=1)
        next_start, _ = session_bounds(next_day)
        return next_start + timedelta(minutes=interval)

    if ts_ist <= start:
        return start + timedelta(minutes=interval)

    delta = ts_ist - start
    minutes = int(delta.total_seconds() // 60)
    next_multiple = ((minutes // interval) + 1) * interval
    candidate = start + timedelta(minutes=next_multiple)
    if candidate <= ts_ist:
        candidate += timedelta(minutes=interval)
    return candidate


def is_within_session(ts: datetime) -> bool:
    """Return ``True`` if ``ts`` lies within the IST trading session."""

    ts_ist = _require_ist(ts)
    start, end = session_bounds(ts_ist.date())
    return start <= ts_ist <= end


def is_bar_complete(ts: datetime, tf: Timeframe) -> bool:
    """Pure check for bar completeness (alignment and session membership)."""

    ts_ist = _require_ist(ts)

    if tf == "1d":
        return is_bar_boundary(ts_ist, tf)

    return is_bar_boundary(ts_ist, tf) and is_within_session(ts_ist)


def within_orb(ts: datetime, start: str = "09:15", end: str = "09:30") -> bool:
    """Return whether ``ts`` falls within the Opening Range Breakout window."""

    ts_ist = _require_ist(ts)

    try:
        start_h, start_m = (int(part) for part in start.split(":", 1))
        end_h, end_m = (int(part) for part in end.split(":", 1))
    except ValueError as exc:  # pragma: no cover - invalid configuration
        raise ValueError("ORB start/end must be in HH:MM format") from exc

    start_dt = datetime.combine(ts_ist.date(), time(start_h, start_m), tzinfo=_IST_ZONE)
    end_dt = datetime.combine(ts_ist.date(), time(end_h, end_m), tzinfo=_IST_ZONE)
    if end_dt <= start_dt:
        raise ValueError("ORB end time must be after start time")
    return start_dt <= ts_ist < end_dt

