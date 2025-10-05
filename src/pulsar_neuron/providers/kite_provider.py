"""Kite + NSE backed market data provider."""

from __future__ import annotations

import json
import logging
import math
import os
import time
from collections import defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path
from random import random
from typing import Any, Iterable

import requests
from kiteconnect import KiteConnect  # type: ignore
from zoneinfo import ZoneInfo

from pulsar_neuron.config.loader import load_defaults, load_markets
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


class KiteMarketProvider(MarketProvider):
    def __init__(self, config: dict[str, Any], logger: logging.Logger | None = None) -> None:
        self._config = config
        self._market_cfg = config.get("market", {})
        self._logger = logger or logging.getLogger(__name__)
        api_key = os.environ["KITE_API_KEY"]
        access_token = os.environ["KITE_ACCESS_TOKEN"]
        self._kite = KiteConnect(api_key=api_key)
        self._kite.set_access_token(access_token)
        tz_name = self._market_cfg.get("tz", "Asia/Kolkata")
        self._tz = ZoneInfo(tz_name)

        defaults = load_defaults().get("market", {})
        retry_cfg = self._market_cfg.get("retries", defaults.get("retries", {}))
        timeout_cfg = self._market_cfg.get("timeouts", defaults.get("timeouts", {}))
        self._max_attempts = int(retry_cfg.get("max_attempts", 3))
        self._base_delay = float(retry_cfg.get("base_delay_ms", 250)) / 1000.0
        self._http_timeout = float(timeout_cfg.get("http", 6))
        self._quote_timeout = float(timeout_cfg.get("quote", 3))

        self._session = requests.Session()
        ua = os.getenv("NSE_USER_AGENT") or (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36"
        )
        self._session.headers.update(
            {
                "User-Agent": ua,
                "Accept": "application/json, text/plain, */*",
                "Referer": "https://www.nseindia.com/",
            }
        )
        self._nse_bootstrapped = False

        markets = load_markets()
        self._tokens_cfg = markets.get("tokens", {})
        self._alias_map = {self._normalize_symbol(k): int(v) for k, v in self._tokens_cfg.items()}
        for key in list(self._alias_map.keys()):
            self._alias_map[key.replace("50", "")] = self._alias_map[key]
        self._instrument_cache_path = Path(".cache/instruments.json")
        self._instrument_cache: dict[str, Any] = {}
        self._future_cache: dict[str, list[dict[str, Any]]] = {}

    def _normalize_symbol(self, symbol: str) -> str:
        return symbol.replace(" ", "").replace("-", "").upper()

    def _retry(self, func, description: str, *args, **kwargs):
        delay = self._base_delay
        last_exc: Exception | None = None
        for attempt in range(1, self._max_attempts + 1):
            try:
                return func(*args, **kwargs)
            except Exception as exc:  # pragma: no cover - network
                last_exc = exc
                self._logger.warning(
                    "Retrying %s (attempt %s/%s): %s",
                    description,
                    attempt,
                    self._max_attempts,
                    exc,
                )
                if attempt == self._max_attempts:
                    break
                sleep_for = delay * (1 + random())
                time.sleep(sleep_for)
                delay *= 2
        assert last_exc is not None
        raise last_exc

    def _ensure_instruments(self) -> None:
        if self._instrument_cache:
            return
        cache_valid = False
        if self._instrument_cache_path.exists():
            try:
                stat = self._instrument_cache_path.stat()
                cache_valid = time.time() - stat.st_mtime < 24 * 3600
                if cache_valid:
                    with self._instrument_cache_path.open("r", encoding="utf-8") as fh:
                        self._instrument_cache = json.load(fh)
            except Exception:
                cache_valid = False
        if not cache_valid:
            instruments = self._retry(self._kite.instruments, "kite.instruments")
            self._instrument_cache = {
                str(inst["instrument_token"]): inst for inst in instruments
            }
            self._instrument_cache_path.parent.mkdir(parents=True, exist_ok=True)
            with self._instrument_cache_path.open("w", encoding="utf-8") as fh:
                json.dump(self._instrument_cache, fh)
        self._future_cache.clear()

    def _resolve_index_token(self, symbol: str) -> int | None:
        self._ensure_instruments()
        alias = self._normalize_symbol(symbol)
        token = self._alias_map.get(alias)
        if token:
            return token
        for inst in self._instrument_cache.values():
            tradingsymbol = inst.get("tradingsymbol", "")
            if self._normalize_symbol(tradingsymbol) == alias:
                return int(inst["instrument_token"])
        self._logger.warning("Unknown index symbol %s", symbol)
        return None

    def _resolve_future_token(self, symbol: str) -> int | None:
        base = self._normalize_symbol(symbol)
        if base in self._future_cache:
            insts = self._future_cache[base]
        else:
            self._ensure_instruments()
            insts = [
                inst
                for inst in self._instrument_cache.values()
                if inst.get("segment") == "NFO-FUT"
                and self._normalize_symbol(inst.get("tradingsymbol", ""))
                .startswith(base)
            ]
            self._future_cache[base] = insts
        if not insts:
            self._logger.warning("No futures instrument found for %s", symbol)
            return None
        today = now_ist().date()
        insts.sort(key=lambda inst: inst.get("expiry"))
        for inst in insts:
            expiry = inst.get("expiry")
            if isinstance(expiry, str):
                expiry_date = datetime.strptime(expiry, "%Y-%m-%d").date()
            elif isinstance(expiry, datetime):
                expiry_date = expiry.date()
            elif isinstance(expiry, date):
                expiry_date = expiry
            else:
                continue
            if expiry_date >= today:
                return int(inst["instrument_token"])
        return int(insts[-1]["instrument_token"])

    def _historical_interval(self, tf: Timeframe) -> str:
        return {"5m": "5minute", "15m": "15minute", "1d": "day"}[tf]

    def fetch_ohlcv(
        self, symbols: Iterable[str], tf: Timeframe, since: datetime | None = None
    ) -> list[OhlcvBar]:
        interval = self._historical_interval(tf)
        to_dt = now_ist().astimezone(self._tz).replace(second=0, microsecond=0)
        if since is None:
            if tf == "1d":
                since = to_dt - timedelta(days=10)
            elif tf == "15m":
                since = to_dt - timedelta(days=7)
            else:
                since = to_dt - timedelta(days=3)
        payload: list[OhlcvBar] = []
        for symbol in symbols:
            token = self._resolve_index_token(symbol)
            if not token:
                continue
            history = self._retry(
                self._kite.historical_data,
                f"historical_data:{symbol}:{tf}",
                token,
                since,
                to_dt,
                interval,
            )
            for bar in history:
                ts = bar["date"]
                if isinstance(ts, datetime):
                    ts_dt = ts.astimezone(self._tz)
                else:
                    ts_dt = datetime.fromisoformat(str(ts)).astimezone(self._tz)
                payload.append(
                    OhlcvBar(
                        symbol=symbol,
                        ts_ist=ts_dt,
                        tf=tf,
                        o=float(bar.get("open", math.nan)),
                        h=float(bar.get("high", math.nan)),
                        l=float(bar.get("low", math.nan)),
                        c=float(bar.get("close", math.nan)),
                        v=int(bar.get("volume", 0)),
                    )
                )
        return payload

    def fetch_fut_oi(self, symbols: Iterable[str]) -> list[FutOiRow]:
        token_map: dict[int, str] = {}
        for symbol in symbols:
            token = self._resolve_future_token(symbol)
            if token:
                token_map[token] = symbol
        if not token_map:
            return []
        quotes = self._retry(self._kite.quote, "fut_quote", list(token_map.keys()))
        now = now_ist().astimezone(self._tz)
        rows: list[FutOiRow] = []
        for token, symbol in token_map.items():
            quote = quotes.get(str(token)) or quotes.get(token) or {}
            price = quote.get("last_price") or quote.get("last_trade_price") or 0.0
            oi = quote.get("oi") or quote.get("open_interest") or 0
            rows.append(
                FutOiRow(
                    symbol=symbol,
                    ts_ist=now,
                    price=float(price),
                    oi=int(oi),
                    baseline_tag=None,
                )
            )
        return rows

    def _ensure_nse_bootstrap(self) -> None:
        if self._nse_bootstrapped:
            return
        self._session.get("https://www.nseindia.com", timeout=self._http_timeout)
        self._nse_bootstrapped = True

    def _fetch_nse_json(self, path: str) -> Any:
        self._ensure_nse_bootstrap()
        url = f"https://www.nseindia.com{path}"
        response = self._retry(
            self._session.get,
            f"nse:{path}",
            url,
            timeout=self._http_timeout,
        )
        response.raise_for_status()
        return response.json()

    def _option_symbol_slug(self, symbol: str) -> str:
        alias = self._normalize_symbol(symbol)
        if alias in {"NIFTY50", "NIFTY"}:
            return "NIFTY"
        if alias in {"BANKNIFTY", "NIFTYBANK"}:
            return "BANKNIFTY"
        return alias

    def fetch_option_chain(self, symbol: str) -> list[OptionRow]:
        slug = self._option_symbol_slug(symbol)
        data = self._fetch_nse_json(f"/api/option-chain-indices?symbol={slug}")
        records = data.get("records", {})
        underlying = float(records.get("underlyingValue") or 0.0)
        entries = records.get("data", [])
        per_expiry: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for row in entries:
            expiry = row.get("expiryDate")
            if expiry:
                per_expiry[str(expiry)].append(row)
        span = int(self._market_cfg.get("options", {}).get("strikes_span", 12))
        expiries_max = int(self._market_cfg.get("options", {}).get("expiries_max", 3))
        now = now_ist().astimezone(self._tz)
        results: list[OptionRow] = []
        for expiry, rows in sorted(per_expiry.items(), key=lambda kv: kv[0])[:expiries_max]:
            strikes = sorted({float(r.get("strikePrice") or 0.0) for r in rows})
            if not strikes:
                continue
            closest_idx = min(range(len(strikes)), key=lambda idx: abs(strikes[idx] - underlying))
            start = max(0, closest_idx - span)
            end = min(len(strikes), closest_idx + span + 1)
            window = set(strikes[start:end])
            for row in rows:
                strike = float(row.get("strikePrice") or 0.0)
                if strike not in window:
                    continue
                for side in ("CE", "PE"):
                    leg = row.get(side) or {}
                    if not leg:
                        continue
                    results.append(
                        OptionRow(
                            symbol=symbol,
                            ts_ist=now,
                            expiry=expiry,
                            strike=strike,
                            side=side,  # type: ignore[arg-type]
                            ltp=float(leg.get("lastPrice") or 0.0),
                            iv=float(leg.get("impliedVolatility") or 0.0),
                            oi=int(leg.get("openInterest") or 0),
                            doi=int(leg.get("changeinOpenInterest") or 0),
                            volume=int(leg.get("totalTradedVolume") or 0),
                            delta=float(leg.get("delta") or 0.0),
                            gamma=float(leg.get("gamma") or 0.0),
                            theta=float(leg.get("theta") or 0.0),
                            vega=float(leg.get("vega") or 0.0),
                        )
                    )
        return results

    def fetch_breadth(self) -> BreadthRow:
        data = self._fetch_nse_json("/api/market-status")
        overall = data.get("marketState", [])
        counts = next((item for item in overall if item.get("market") == "NSE"), {})
        advance = int(counts.get("advances") or 0)
        decline = int(counts.get("declines") or 0)
        unchanged = int(counts.get("unchanged") or 0)
        return BreadthRow(ts_ist=now_ist().astimezone(self._tz), adv=advance, dec=decline, unchanged=unchanged)

    def fetch_vix(self) -> VixRow:
        data = self._fetch_nse_json("/api/allIndices")
        for entry in data.get("data", []):
            if entry.get("index") == "INDIA VIX":
                return VixRow(
                    ts_ist=now_ist().astimezone(self._tz),
                    value=float(entry.get("last") or entry.get("lastPrice") or 0.0),
                )
        return VixRow(ts_ist=now_ist().astimezone(self._tz), value=0.0)
