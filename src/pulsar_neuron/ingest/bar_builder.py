from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, time, timedelta
from typing import Dict, Optional, Literal, List, Tuple
from zoneinfo import ZoneInfo

Timeframe = Literal["5m", "15m", "1d"]  # NOTE: BarBuilder currently supports only "5m"

IST = ZoneInfo("Asia/Kolkata")
SESSION_START = time(9, 15)
SESSION_END = time(15, 30)

# ---- time helpers ------------------------------------------------------------

def _now_ist() -> datetime:
    return datetime.now(tz=IST)

def _as_ist(dt: datetime) -> datetime:
    return dt.astimezone(IST) if dt.tzinfo else dt.replace(tzinfo=IST)

def _session_bounds(d: datetime) -> Tuple[datetime, datetime]:
    d = _as_ist(d)
    return (
        d.replace(hour=SESSION_START.hour, minute=SESSION_START.minute, second=0, microsecond=0),
        d.replace(hour=SESSION_END.hour, minute=SESSION_END.minute, second=0, microsecond=0),
    )

def _next_5m_end(after: datetime) -> datetime:
    """
    Return the next 5m bar END timestamp in IST session, strictly > `after`.
    First end is 09:20; then every 5 minutes; past 15:30 → next day 09:20.
    """
    d = _as_ist(after)
    start, end = _session_bounds(d)

    # If before session start → first end is 09:20 today
    if d < start:
        return start + timedelta(minutes=5)

    # If at/after session end → first end is 09:20 next trading day
    if d >= end:
        nd = (d + timedelta(days=1)).replace(
            hour=SESSION_START.hour, minute=SESSION_START.minute, second=0, microsecond=0
        )
        return nd + timedelta(minutes=5)

    # Inside session: compute next multiple of 5 strictly greater than d
    mins = int((d - start).total_seconds() // 60)
    # minute offset within the session window (0 for 09:15:xx, 1 for 09:16:xx, ...)
    # next bucket end must be strictly greater than 'd', so add 1 first
    next_bucket = ((mins // 5) + 1) * 5
    candidate = start + timedelta(minutes=next_bucket)

    # If we're exactly on a boundary (e.g., 09:20:00), above logic already gives > d.
    # If candidate spills past session end, push to next day 09:20
    if candidate > end:
        nd = (d + timedelta(days=1)).replace(
            hour=SESSION_START.hour, minute=SESSION_START.minute, second=0, microsecond=0
        )
        return nd + timedelta(minutes=5)

    return candidate

def _advance_5m_end(prev_end: datetime) -> datetime:
    """
    Advance a 5m end to the next valid 5m end respecting the session window.
    If prev_end == 15:30, the next end is next day 09:20.
    """
    prev_end = _as_ist(prev_end)
    start, end = _session_bounds(prev_end)
    nxt = prev_end + timedelta(minutes=5)
    if nxt > end:
        nd = (prev_end + timedelta(days=1)).replace(
            hour=SESSION_START.hour, minute=SESSION_START.minute, second=0, microsecond=0
        )
        return nd + timedelta(minutes=5)
    return nxt

# ---- types & builder ---------------------------------------------------------

@dataclass
class BarState:
    ts_end: datetime  # IST bar end
    o: float
    h: float
    l: float
    c: float
    v: int

class BarBuilder:
    """
    Tick → 5m OHLCV aggregator with strict IST bar-end semantics.

    - Feed ticks via on_tick().
    - Call maybe_close(now?) ~1/s (or on a scheduler). It can emit 0..N completed bars.
    - Ticks arriving exactly on a boundary (e.g., 09:20:00) belong to the NEXT bar.
    - If the clock/jitter skips, missing bars are backfilled as carry-forward
      (o=h=l=c=previous close, v=0) so downstream logic gets contiguous bars.
    """

    def __init__(self, symbols: List[str], tf: Timeframe = "5m"):
        assert tf == "5m", "BarBuilder currently builds 5m bars only."
        self.tf = tf
        self.state: Dict[str, Optional[BarState]] = {s: None for s in symbols}

    def _ensure_bucket(self, symbol: str, now_ist: datetime, price: float):
        st = self.state[symbol]
        if st is None:
            # IMPORTANT: pass 'now_ist' (not minus 1ms) so boundary ticks start next bar
            ts_end = _next_5m_end(now_ist)
            self.state[symbol] = BarState(
                ts_end=ts_end, o=price, h=price, l=price, c=price, v=0
            )

    def on_tick(self, symbol: str, price: float, vol: Optional[int] = None, ts: Optional[datetime] = None):
        now = _as_ist(ts) if ts else _now_ist()
        self._ensure_bucket(symbol, now, price)
        st = self.state[symbol]
        assert st is not None
        if price > st.h:
            st.h = price
        if price < st.l:
            st.l = price
        st.c = price
        if vol is not None:
            # Coerce to non-negative int
            st.v += int(max(vol, 0))

    def _emit_bar(self, symbol: str, st: BarState) -> dict:
        return {
            "symbol": symbol,
            "tf": self.tf,
            "ts_ist": _as_ist(st.ts_end),
            "o": float(st.o),
            "h": float(st.h),
            "l": float(st.l),
            "c": float(st.c),
            "v": int(st.v),
        }

    def maybe_close(self, now: Optional[datetime] = None) -> List[dict]:
        """
        Emit all due bars up to 'now'. If multiple 5m windows elapsed since last tick,
        this will produce multiple carry-forward bars (v=0) to keep continuity.
        """
        now_ist = _as_ist(now) if now else _now_ist()
        out: List[dict] = []

        for symbol, st in self.state.items():
            if st is None:
                continue

            # If we're overdue, flush as many full bars as needed
            while now_ist >= st.ts_end:
                out.append(self._emit_bar(symbol, st))

                # carry forward close into next bar; zero volume
                next_end = _advance_5m_end(st.ts_end)
                prev_close = st.c
                st = BarState(
                    ts_end=next_end, o=prev_close, h=prev_close, l=prev_close, c=prev_close, v=0
                )
                self.state[symbol] = st

        return out
