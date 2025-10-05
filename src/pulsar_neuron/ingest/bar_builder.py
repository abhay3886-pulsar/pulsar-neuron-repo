# src/pulsar_neuron/ingest/bar_builder.py
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, time, timedelta
from typing import Dict, Optional, Literal, List, Tuple
from zoneinfo import ZoneInfo

# ---- constants ---------------------------------------------------------------

Timeframe = Literal["5m", "15m", "1d"]  # NOTE: BarBuilder currently supports only "5m"

IST = ZoneInfo("Asia/Kolkata")
SESSION_START = time(9, 15)
SESSION_END = time(15, 30)

# ---- time helpers ------------------------------------------------------------

def _now_ist() -> datetime:
    """Return current IST time (aware)."""
    return datetime.now(tz=IST)

def _as_ist(dt: datetime) -> datetime:
    """Ensure datetime is tz-aware in IST."""
    return dt.astimezone(IST) if dt.tzinfo else dt.replace(tzinfo=IST)

def _session_bounds(d: datetime) -> Tuple[datetime, datetime]:
    """Return (session_start, session_end) for a given day in IST."""
    d = _as_ist(d)
    return (
        d.replace(hour=SESSION_START.hour, minute=SESSION_START.minute, second=0, microsecond=0),
        d.replace(hour=SESSION_END.hour, minute=SESSION_END.minute, second=0, microsecond=0),
    )

def _next_5m_end(after: datetime) -> datetime:
    """
    Return the next 5-minute bar END timestamp in IST, strictly > `after`.
    - First end = 09:20
    - Every 5 minutes thereafter until 15:30
    - If beyond 15:30 → next day 09:20
    """
    d = _as_ist(after)
    start, end = _session_bounds(d)

    # Before session start → 09:20 today
    if d < start:
        return start + timedelta(minutes=5)

    # After session end → next trading day 09:20
    if d >= end:
        nd = (d + timedelta(days=1)).replace(
            hour=SESSION_START.hour, minute=SESSION_START.minute, second=0, microsecond=0
        )
        return nd + timedelta(minutes=5)

    # Inside session → compute next multiple of 5 strictly greater than 'd'
    mins = int((d - start).total_seconds() // 60)
    next_bucket = ((mins // 5) + 1) * 5
    candidate = start + timedelta(minutes=next_bucket)

    # Spill-over safety
    if candidate > end:
        nd = (d + timedelta(days=1)).replace(
            hour=SESSION_START.hour, minute=SESSION_START.minute, second=0, microsecond=0
        )
        return nd + timedelta(minutes=5)

    return candidate

def _advance_5m_end(prev_end: datetime) -> datetime:
    """
    Advance a 5-minute bar end to the next valid end inside session.
    If prev_end == 15:30, roll to next day 09:20.
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

# ---- data classes ------------------------------------------------------------

@dataclass
class BarState:
    ts_end: datetime  # IST bar end
    o: float
    h: float
    l: float
    c: float
    v: int

# ---- builder -----------------------------------------------------------------

class BarBuilder:
    """
    Tick → 5-minute OHLCV aggregator with strict IST bar semantics.

    Usage:
      builder = BarBuilder(["NIFTY 50", "NIFTY BANK"])
      builder.on_tick("NIFTY 50", 22150.5, vol=100)
      closed = builder.maybe_close(datetime.now(IST))
      if closed: upsert_to_db(closed)

    Notes:
      - Ticks exactly at boundary (e.g. 09:20:00) belong to the *next* bar.
      - If scheduler skips time, missing bars are back-filled (v=0).
      - Volume is summed incrementally (no diff logic).
    """

    def __init__(self, symbols: List[str], tf: Timeframe = "5m"):
        assert tf == "5m", "BarBuilder currently supports only 5-minute bars."
        self.tf = tf
        self.state: Dict[str, Optional[BarState]] = {s: None for s in symbols}

    # ----------------------------------------------------------------------

    def _ensure_bucket(self, symbol: str, now_ist: datetime, price: float):
        st = self.state[symbol]
        if st is None:
            ts_end = _next_5m_end(now_ist)
            self.state[symbol] = BarState(
                ts_end=ts_end, o=price, h=price, l=price, c=price, v=0
            )

    def on_tick(self, symbol: str, price: float, vol: Optional[int] = None, ts: Optional[datetime] = None):
        """Feed a tick (price + optional volume) into the builder."""
        now = _as_ist(ts) if ts else _now_ist()
        self._ensure_bucket(symbol, now, price)
        st = self.state[symbol]
        assert st is not None

        # update OHLC
        if price > st.h:
            st.h = price
        if price < st.l:
            st.l = price
        st.c = price

        # update volume
        if vol is not None:
            st.v += int(max(vol, 0))

    def _emit_bar(self, symbol: str, st: BarState) -> dict:
        """Convert current BarState into a serializable OHLCV dict."""
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
        Emit all completed bars up to 'now'.
        Returns a list of OHLCV dicts (possibly empty).
        Back-fills missed bars to maintain continuity.
        """
        now_ist = _as_ist(now) if now else _now_ist()
        out: List[dict] = []

        for symbol, st in self.state.items():
            if st is None:
                continue

            # flush all overdue bars
            while now_ist >= st.ts_end:
                out.append(self._emit_bar(symbol, st))
                next_end = _advance_5m_end(st.ts_end)
                prev_close = st.c
                st = BarState(
                    ts_end=next_end,
                    o=prev_close,
                    h=prev_close,
                    l=prev_close,
                    c=prev_close,
                    v=0,
                )
                self.state[symbol] = st

        return out
