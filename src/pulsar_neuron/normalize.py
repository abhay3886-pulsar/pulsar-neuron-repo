"""Normalization helpers for ingest jobs."""

from __future__ import annotations

import logging
import math
from datetime import datetime
from typing import Iterable
from zoneinfo import ZoneInfo

from pulsar_neuron.config.loader import load_defaults
from pulsar_neuron.providers.market_provider import (
    BreadthRow,
    FutOiRow,
    OhlcvBar,
    OptionRow,
    Timeframe,
    VixRow,
)
from pulsar_neuron.timeutils import is_bar_complete

logger = logging.getLogger(__name__)
_cfg = load_defaults()
_market_cfg = _cfg.get("market", {})
_tz = ZoneInfo(_market_cfg.get("tz", "Asia/Kolkata"))
_allowed_tfs = set(_market_cfg.get("ohlcv", {}).get("tfs", ["5m", "15m", "1d"]))
_bar_delay = int(_market_cfg.get("ohlcv", {}).get("bar_complete_delay_s", 10))


def _parse_ts(value: datetime | str) -> datetime:
    if isinstance(value, datetime):
        dt = value
    elif isinstance(value, str):
        txt = value.replace("Z", "+00:00")
        dt = datetime.fromisoformat(txt)
    else:
        raise TypeError(f"Unsupported ts type: {type(value)!r}")
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=_tz)
    else:
        dt = dt.astimezone(_tz)
    return dt


def normalize_ohlcv(bars: Iterable[OhlcvBar]) -> list[OhlcvBar]:
    normalized: list[OhlcvBar] = []
    seen: set[tuple[str, datetime, Timeframe]] = set()
    for bar in bars:
        try:
            symbol = str(bar["symbol"]).strip()
            tf = str(bar["tf"])
            if tf not in _allowed_tfs:
                raise ValueError(f"unsupported tf {tf}")
            ts = _parse_ts(bar["ts_ist"])
            o = float(bar["o"])
            h = float(bar["h"])
            l = float(bar["l"])
            c = float(bar["c"])
            v = int(bar["v"])
        except Exception as exc:
            logger.warning("Dropping OHLCV row due to error: %s", exc)
            continue
        key = (symbol, ts, tf)  # type: ignore[arg-type]
        if key in seen:
            logger.warning("Duplicate OHLCV row skipped %s %s %s", symbol, tf, ts.isoformat())
            continue
        if not is_bar_complete(ts, tf, _bar_delay):
            logger.info("⏳ partial bar skipped %s %s %s", symbol, tf, ts.isoformat())
            continue
        if not math.isfinite(o + h + l + c):
            logger.warning("Invalid OHLCV for %s %s at %s", symbol, tf, ts.isoformat())
            continue
        seen.add(key)
        normalized.append(
            OhlcvBar(
                symbol=symbol,
                tf=tf,  # type: ignore[arg-type]
                ts_ist=ts,
                o=round(o, 4),
                h=round(h, 4),
                l=round(l, 4),
                c=round(c, 4),
                v=v,
            )
        )
    return normalized


def normalize_fut_oi(rows: Iterable[FutOiRow]) -> list[FutOiRow]:
    normalized: list[FutOiRow] = []
    seen: set[tuple[str, datetime]] = set()
    for row in rows:
        try:
            symbol = str(row["symbol"]).strip()
            ts = _parse_ts(row["ts_ist"])
            price = round(float(row["price"]), 4)
            oi = int(row["oi"])
            tag = row.get("baseline_tag")
            baseline_tag = str(tag) if tag else None
        except Exception as exc:
            logger.warning("Dropping fut OI row: %s", exc)
            continue
        key = (symbol, ts)
        if key in seen:
            continue
        seen.add(key)
        normalized.append(
            FutOiRow(
                symbol=symbol,
                ts_ist=ts,
                price=price,
                oi=oi,
                baseline_tag=baseline_tag,
            )
        )
    return normalized


def _clip(value: float | None, minimum: float, maximum: float, label: str, context: str) -> float | None:
    if value is None or not math.isfinite(value):
        return None
    clipped = min(max(value, minimum), maximum)
    if clipped != value:
        logger.warning("Clipped %s (%s) from %s to %s", label, context, value, clipped)
    return clipped


def normalize_option_chain(rows: Iterable[OptionRow]) -> list[OptionRow]:
    normalized: list[OptionRow] = []
    seen: set[tuple[str, datetime, str, float, str]] = set()
    for row in rows:
        try:
            symbol = str(row["symbol"]).strip()
            ts = _parse_ts(row["ts_ist"])
            expiry = str(row["expiry"])
            strike = float(row["strike"])
            side = str(row["side"]).upper()
            if side not in {"CE", "PE"}:
                raise ValueError("invalid option side")
            ltp = round(float(row["ltp"]), 4)
            iv = row.get("iv")
            oi = row.get("oi")
            doi = row.get("doi")
            volume = row.get("volume")
            delta = row.get("delta")
            gamma = row.get("gamma")
            theta = row.get("theta")
            vega = row.get("vega")
        except Exception as exc:
            logger.warning("Dropping option row: %s", exc)
            continue
        key = (symbol, ts, expiry, strike, side)
        if key in seen:
            continue
        seen.add(key)
        normalized.append(
            OptionRow(
                symbol=symbol,
                ts_ist=ts,
                expiry=expiry,
                strike=round(strike, 2),
                side=side,  # type: ignore[arg-type]
                ltp=ltp,
                iv=_clip(iv, 0.0, 200.0, "iv", symbol),
                oi=int(oi) if oi is not None else None,
                doi=int(doi) if doi is not None else None,
                volume=int(volume) if volume is not None else None,
                delta=_clip(delta, -1.0, 1.0, "delta", symbol),
                gamma=_clip(gamma, -10.0, 10.0, "gamma", symbol),
                theta=_clip(theta, -1000.0, 1000.0, "theta", symbol),
                vega=_clip(vega, -1000.0, 1000.0, "vega", symbol),
            )
        )
    return normalized


def normalize_breadth(row: BreadthRow) -> BreadthRow:
    ts = _parse_ts(row["ts_ist"])
    adv = int(row["adv"])
    dec = int(row["dec"])
    unchanged = int(row.get("unchanged", 0))
    return BreadthRow(ts_ist=ts, adv=adv, dec=dec, unchanged=unchanged)


def normalize_vix(row: VixRow) -> VixRow:
    ts = _parse_ts(row["ts_ist"])
    value = float(row["value"])
    return VixRow(ts_ist=ts, value=round(value, 4))


__all__ = [
    "normalize_ohlcv",
    "normalize_fut_oi",
    "normalize_option_chain",
    "normalize_breadth",
    "normalize_vix",
]
