from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, time, timedelta
from typing import Dict, Optional, Literal, List
from zoneinfo import ZoneInfo

Timeframe = Literal["5m", "15m", "1d"]

IST = ZoneInfo("Asia/Kolkata")
SESSION_START = time(9, 15)
SESSION_END = time(15, 30)


def _now_ist() -> datetime:
    return datetime.now(tz=IST)


def _session_bounds(d: datetime) -> tuple[datetime, datetime]:
    return (
        d.replace(hour=SESSION_START.hour, minute=SESSION_START.minute, second=0, microsecond=0),
        d.replace(hour=SESSION_END.hour, minute=SESSION_END.minute, second=0, microsecond=0),
    )


def _next_5m_end(after: datetime) -> datetime:
    """Return the next 5m bar END timestamp in IST session.
    First end is 09:20; then every 5 minutes; past 15:30 → next day 09:20.
    """
    d = after.astimezone(IST)
    start, end = _session_bounds(d)
    if d < start:
        return start + timedelta(minutes=5)
    if d >= end:
        nd = (d + timedelta(days=1)).replace(
            hour=SESSION_START.hour, minute=SESSION_START.minute, second=0, microsecond=0
        )
        return nd + timedelta(minutes=5)
    mins = int((d - start).total_seconds() // 60)
    next_bucket = (mins // 5 + 1) * 5
    return start + timedelta(minutes=next_bucket)


@dataclass
class BarState:
    ts_end: datetime  # IST bar end
    o: float
    h: float
    l: float
    c: float
    v: int


class BarBuilder:
    """Tick → 5m OHLCV aggregator with strict IST bar-end semantics.
    Feed ticks via on_tick(); call maybe_close() ~1/s; it emits completed bars.
    """

    def __init__(self, symbols: List[str], tf: Timeframe = "5m"):
        assert tf == "5m", "BarBuilder currently builds 5m bars only."
        self.tf = tf
        self.state: Dict[str, Optional[BarState]] = {s: None for s in symbols}

    def _ensure_bucket(self, symbol: str, now_ist: datetime, price: float):
        st = self.state[symbol]
        if st is None:
            ts_end = _next_5m_end(now_ist - timedelta(milliseconds=1))
            self.state[symbol] = BarState(
                ts_end=ts_end, o=price, h=price, l=price, c=price, v=0
            )

    def on_tick(self, symbol: str, price: float, vol: Optional[int] = None, ts: Optional[datetime] = None):
        now = ts.astimezone(IST) if ts else _now_ist()
        self._ensure_bucket(symbol, now, price)
        st = self.state[symbol]
        assert st is not None
        if price > st.h:
            st.h = price
        if price < st.l:
            st.l = price
        st.c = price
        if vol:
            st.v += int(max(vol, 0))

    def maybe_close(self, now: Optional[datetime] = None) -> List[dict]:
        now = now.astimezone(IST) if now else _now_ist()
        out: List[dict] = []
        for symbol, st in self.state.items():
            if st is None:
                continue
            if now >= st.ts_end:
                out.append(
                    {
                        "symbol": symbol,
                        "tf": self.tf,
                        "ts_ist": st.ts_end,
                        "o": float(st.o),
                        "h": float(st.h),
                        "l": float(st.l),
                        "c": float(st.c),
                        "v": int(st.v),
                    }
                )
                next_end = st.ts_end + timedelta(minutes=5)
                self.state[symbol] = BarState(
                    ts_end=next_end, o=st.c, h=st.c, l=st.c, c=st.c, v=0
                )
        return out
