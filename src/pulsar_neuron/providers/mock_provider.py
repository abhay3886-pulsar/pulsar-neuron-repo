"""Deterministic mock market provider used for offline development."""

from __future__ import annotations

import math
import random
from datetime import datetime, timedelta
from typing import Iterable
from zoneinfo import ZoneInfo

from pulsar_neuron.providers.market_provider import (
    BreadthRow,
    FutOiRow,
    MarketProvider,
    OhlcvBar,
    OptionRow,
    Timeframe,
    VixRow,
)
from pulsar_neuron.timeutils import now_ist


class MockMarketProvider(MarketProvider):
    def __init__(self, tz: str = "Asia/Kolkata") -> None:
        self._tz = ZoneInfo(tz)

    def _rng(self, key: str) -> random.Random:
        seed = abs(hash(key)) % (2**32)
        return random.Random(seed)

    def _base_price(self, symbol: str) -> float:
        upper = symbol.upper()
        if "BANK" in upper:
            return 43500.0
        if "FIN" in upper:
            return 19000.0
        return 22500.0

    def fetch_ohlcv(
        self, symbols: Iterable[str], tf: Timeframe, since: datetime | None = None
    ) -> list[OhlcvBar]:
        symbols = list(symbols)
        if not symbols:
            return []

        now = now_ist().astimezone(self._tz)
        if tf == "1d":
            step = timedelta(days=1)
        elif tf == "15m":
            step = timedelta(minutes=15)
        else:
            step = timedelta(minutes=5)

        bars: list[OhlcvBar] = []
        for symbol in symbols:
            rng = self._rng(f"ohlcv:{symbol}:{tf}")
            base = self._base_price(symbol)
            end = now - step
            for _ in range(6):
                ts = end.replace(second=0, microsecond=0)
                if tf != "1d":
                    minute = (ts.minute // int(step.total_seconds() // 60)) * int(
                        step.total_seconds() // 60
                    )
                    ts = ts.replace(minute=minute)
                if since and ts <= since:
                    break
                drift = math.sin(ts.timestamp() / 3600.0) * 15.0
                o = base + drift + rng.uniform(-20.0, 20.0)
                h = o + abs(rng.gauss(6, 2))
                l = o - abs(rng.gauss(6, 2))
                c = l + (h - l) * rng.random()
                volume = int(abs(rng.gauss(1_500_000, 250_000)))
                bars.append(
                    OhlcvBar(
                        symbol=symbol,
                        tf=tf,
                        ts_ist=ts.astimezone(self._tz),
                        o=float(o),
                        h=float(h),
                        l=float(l),
                        c=float(c),
                        v=volume,
                    )
                )
                end = ts - step
        bars.sort(key=lambda b: (b["symbol"], b["ts_ist"]))
        return bars

    def fetch_fut_oi(self, symbols: Iterable[str]) -> list[FutOiRow]:
        rows: list[FutOiRow] = []
        now = now_ist().astimezone(self._tz)
        for symbol in symbols:
            rng = self._rng(f"futoi:{symbol}")
            base = self._base_price(symbol)
            price = base + rng.uniform(-50.0, 50.0)
            oi = int(2_000_000 + rng.uniform(-150_000, 150_000))
            rows.append(
                FutOiRow(
                    symbol=symbol,
                    ts_ist=now,
                    price=float(price),
                    oi=oi,
                    baseline_tag=None,
                )
            )
        return rows

    def fetch_option_chain(self, symbol: str) -> list[OptionRow]:
        now = now_ist().astimezone(self._tz)
        base = self._base_price(symbol)
        atm = round(base / 50.0) * 50.0
        rng = self._rng(f"options:{symbol}")
        rows: list[OptionRow] = []
        expiries = [
            (now + timedelta(days=7 * i)).date().isoformat() for i in range(1, 4)
        ]
        strikes = [atm + 50 * offset for offset in range(-12, 13)]
        for expiry in expiries:
            for strike in strikes:
                intrinsic = max(0.0, atm - strike)
                ce_price = max(1.0, intrinsic + rng.uniform(2.0, 25.0))
                pe_price = max(1.0, max(0.0, strike - atm) + rng.uniform(2.0, 25.0))
                iv = max(10.0, min(45.0 + rng.uniform(-5.0, 5.0), 120.0))
                change = int(rng.uniform(-20_000, 20_000))
                volume = int(abs(rng.gauss(150_000, 40_000)))
                greeks = {
                    "delta": max(-1.0, min(1.0, rng.uniform(-1.0, 1.0))),
                    "gamma": rng.uniform(-0.02, 0.02),
                    "theta": rng.uniform(-10.0, 0.0),
                    "vega": rng.uniform(0.0, 15.0),
                }
                rows.append(
                    OptionRow(
                        symbol=symbol,
                        ts_ist=now,
                        expiry=expiry,
                        strike=float(strike),
                        side="CE",
                        ltp=float(ce_price),
                        iv=float(iv),
                        oi=int(200_000 + rng.uniform(-50_000, 50_000)),
                        doi=change,
                        volume=volume,
                        delta=greeks["delta"],
                        gamma=greeks["gamma"],
                        theta=greeks["theta"],
                        vega=greeks["vega"],
                    )
                )
                rows.append(
                    OptionRow(
                        symbol=symbol,
                        ts_ist=now,
                        expiry=expiry,
                        strike=float(strike),
                        side="PE",
                        ltp=float(pe_price),
                        iv=float(iv + rng.uniform(-3.0, 3.0)),
                        oi=int(200_000 + rng.uniform(-50_000, 50_000)),
                        doi=-change,
                        volume=volume,
                        delta=-greeks["delta"],
                        gamma=greeks["gamma"],
                        theta=greeks["theta"],
                        vega=greeks["vega"],
                    )
                )
        return rows

    def fetch_breadth(self) -> BreadthRow:
        now = now_ist().astimezone(self._tz)
        rng = self._rng("breadth")
        adv = int(900 + rng.uniform(-60, 60))
        dec = int(700 + rng.uniform(-60, 60))
        unchanged = max(0, int(100 + rng.uniform(-20, 20)))
        return BreadthRow(ts_ist=now, adv=adv, dec=dec, unchanged=unchanged)

    def fetch_vix(self) -> VixRow:
        now = now_ist().astimezone(self._tz)
        rng = self._rng("vix")
        value = 13.0 + rng.uniform(-1.0, 1.5)
        return VixRow(ts_ist=now, value=float(value))
