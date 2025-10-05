"""Market data provider protocol and shared data structures."""

from __future__ import annotations

from datetime import datetime
from typing import Iterable, Literal, Protocol, TypedDict

Timeframe = Literal["1d", "15m", "5m"]


class OhlcvBar(TypedDict):
    symbol: str
    ts_ist: datetime
    tf: Timeframe
    o: float
    h: float
    l: float
    c: float
    v: int


class FutOiRow(TypedDict, total=False):
    symbol: str
    ts_ist: datetime
    price: float
    oi: int
    baseline_tag: str | None


class OptionRow(TypedDict, total=False):
    symbol: str
    ts_ist: datetime
    expiry: str
    strike: float
    side: Literal["CE", "PE"]
    ltp: float
    iv: float | None
    oi: int | None
    doi: int | None
    volume: int | None
    delta: float | None
    gamma: float | None
    theta: float | None
    vega: float | None


class BreadthRow(TypedDict):
    ts_ist: datetime
    adv: int
    dec: int
    unchanged: int


class VixRow(TypedDict):
    ts_ist: datetime
    value: float


class MarketProvider(Protocol):
    """Interface that every market data provider implementation must satisfy."""

    def fetch_ohlcv(
        self, symbols: Iterable[str], tf: Timeframe, since: datetime | None = None
    ) -> list[OhlcvBar]:
        ...

    def fetch_fut_oi(self, symbols: Iterable[str]) -> list[FutOiRow]:
        ...

    def fetch_option_chain(self, symbol: str) -> list[OptionRow]:
        ...

    def fetch_breadth(self) -> BreadthRow:
        ...

    def fetch_vix(self) -> VixRow:
        ...
