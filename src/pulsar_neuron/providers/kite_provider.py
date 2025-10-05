"""Kite-only market data provider (with IV & Greeks; with LTP & WebSocket)."""

from __future__ import annotations

import json
import logging
import math
import os
import time
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone, time as dtime
from pathlib import Path
from random import random
from typing import Any, Iterable, List, Dict, Tuple, Optional

from kiteconnect import KiteConnect, KiteTicker  # ✅ include both
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
from pulsar_neuron.lib.bs_iv_greeks import implied_vol, bs_greeks, year_fraction  # type: ignore


class KiteMarketProvider(MarketProvider):
    """Unified Zerodha Kite market data provider with IV/Greeks and live tick support."""

    def __init__(self, config: dict[str, Any], logger: logging.Logger | None = None) -> None:
        self._config = config
        self._market_cfg = config.get("market", {}) if isinstance(config, dict) else {}
        self._logger = logger or logging.getLogger(__name__)

        api_key = os.getenv("KITE_API_KEY")
        access_token = os.getenv("KITE_ACCESS_TOKEN")
        if not api_key or not access_token:
            raise RuntimeError("KITE_API_KEY / KITE_ACCESS_TOKEN env vars are required for KiteMarketProvider.")
        self._kite = KiteConnect(api_key=api_key)
        self._kite.set_access_token(access_token)

        tz_name = self._market_cfg.get("tz", "Asia/Kolkata")
        self._tz = ZoneInfo(tz_name)

        defaults = (load_defaults().get("market", {}) if isinstance(load_defaults(), dict) else {})
        retry_cfg = self._market_cfg.get("retries", defaults.get("retries", {}))
        self._max_attempts = int(retry_cfg.get("max_attempts", 3))
        self._base_delay = float(retry_cfg.get("base_delay_ms", 250)) / 1000.0

        markets = load_markets()
        self._tokens_cfg = markets.get("tokens", {})  # e.g. {"NIFTY 50": 256265}
        self._alias_map = {self._norm(k): int(v) for k, v in self._tokens_cfg.items()}
        for key in list(self._alias_map.keys()):
            self._alias_map[key.replace("50", "")] = self._alias_map[key]

        self._instrument_cache_path = Path(".cache/instruments.json")
        self._instrument_cache: Dict[str, Any] = {}
        self._opt_cache: Dict[str, List[Dict[str, Any]]] = {}
        self._fut_cache: Dict[str, List[Dict[str, Any]]] = {}
        self._index_symbol_map: Dict[str, int] = {}

        self._ensure_instruments()

        # Pricing assumptions
        opt_cfg = self._market_cfg.get("options", {}) if isinstance(self._market_cfg, dict) else {}
        self._risk_free_rate = float(opt_cfg.get("risk_free_rate_annual", 0.065))  # 6.5%
        self._div_yield = float(opt_cfg.get("dividend_yield_annual", 0.0))         # 0% for indices

    # ---------------------------------------------------------------------- #
    # Utilities
    # ---------------------------------------------------------------------- #

    def _norm(self, s: str) -> str:
        return s.replace(" ", "").replace("-", "").upper()

    def _retry(self, func, desc: str, *args, **kwargs):
        delay = self._base_delay
        last_exc: Exception | None = None
        for attempt in range(1, self._max_attempts + 1):
            try:
                return func(*args, **kwargs)
            except Exception as exc:
                last_exc = exc
                self._logger.warning("Retrying %s (%d/%d): %s", desc, attempt, self._max_attempts, exc)
                if attempt >= self._max_attempts:
                    break
                time.sleep(delay * (1.0 + random()))
                delay *= 2
        assert last_exc is not None
        raise last_exc

    def _ensure_instruments(self) -> None:
        """Load or refresh the instrument cache."""
        if self._instrument_cache:
            return
        if self._instrument_cache_path.exists():
            try:
                with self._instrument_cache_path.open("r", encoding="utf-8") as fh:
                    self._instrument_cache = json.load(fh)
            except Exception:
                self._instrument_cache = {}

        if not self._instrument_cache:
            instruments = self._retry(self._kite.instruments, "kite.instruments")
            self._instrument_cache = {str(inst["instrument_token"]): inst for inst in instruments}
            self._instrument_cache_path.parent.mkdir(parents=True, exist_ok=True)
            with self._instrument_cache_path.open("w", encoding="utf-8") as fh:
                json.dump(self._instrument_cache, fh)

        # Build index symbol lookup
        for inst in self._instrument_cache.values():
            if inst.get("segment") in ("INDICES", "NSE-INDICES", "NSE"):
                name = self._norm(inst.get("tradingsymbol", "") or inst.get("name", ""))
                if name and "VIX" in name:
                    self._index_symbol_map["INDIAVIX"] = int(inst["instrument_token"])
                if name:
                    self._index_symbol_map[name] = int(inst["instrument_token"])

        self._opt_cache.clear()
        self._fut_cache.clear()

    # ---------------------------------------------------------------------- #
    # Token resolution
    # ---------------------------------------------------------------------- #

    def _resolve_index_token(self, symbol: str) -> int | None:
        alias = self._norm(symbol)
        token = self._alias_map.get(alias) or self._index_symbol_map.get(alias)
        if token:
            return token
        for inst in self._instrument_cache.values():
            if self._norm(inst.get("tradingsymbol", "")) == alias:
                return int(inst["instrument_token"])
        self._logger.warning("Unknown index symbol %s", symbol)
        return None

    def _resolve_future_token(self, symbol: str) -> int | None:
        base = self._norm(symbol)
        if base in self._fut_cache:
            insts = self._fut_cache[base]
        else:
            insts = [
                inst for inst in self._instrument_cache.values()
                if inst.get("segment") == "NFO-FUT"
                and self._norm(inst.get("tradingsymbol", "")).startswith(base)
            ]
            self._fut_cache[base] = insts
        if not insts:
            self._logger.warning("No futures instrument found for %s", symbol)
            return None

        def _to_date(x):
            if isinstance(x, date): return x
            if isinstance(x, datetime): return x.date()
            if isinstance(x, str):
                try: return datetime.fromisoformat(x).date()
                except Exception: return None
            return None

        today = now_ist().date()
        insts = sorted(insts, key=lambda inst: (_to_date(inst.get("expiry")) or date.max))
        for inst in insts:
            exp = _to_date(inst.get("expiry"))
            if exp and exp >= today:
                return int(inst["instrument_token"])
        return int(insts[-1]["instrument_token"])

    # ---------------------------------------------------------------------- #
    # Historical OHLCV
    # ---------------------------------------------------------------------- #

    def _historical_interval(self, tf: Timeframe) -> str:
        return {"5m": "5minute", "15m": "15minute", "1d": "day"}[tf]

    def fetch_ohlcv(self, symbols: Iterable[str], tf: Timeframe, since: datetime | None = None) -> list[OhlcvBar]:
        interval = self._historical_interval(tf)
        to_dt = datetime.now(timezone.utc).replace(second=0, microsecond=0)
        if since is None:
            since = to_dt - timedelta(days={"1d": 10, "15m": 7, "5m": 3}[tf])
        elif since.tzinfo is None:
            since = since.replace(tzinfo=timezone.utc)

        out: list[OhlcvBar] = []
        for symbol in symbols:
            token = self._resolve_index_token(symbol)
            if not token:
                continue
            history = self._retry(self._kite.historical_data, f"historical_data:{symbol}:{tf}", token, since, to_dt, interval)
            for bar in history:
                ts = bar.get("date")
                ts_dt = ts.astimezone(self._tz) if isinstance(ts, datetime) else datetime.fromisoformat(str(ts)).astimezone(self._tz)
                out.append(
                    OhlcvBar(
                        symbol=symbol,
                        ts_ist=ts_dt,
                        tf=tf,
                        o=float(bar.get("open", math.nan)),
                        h=float(bar.get("high", math.nan)),
                        l=float(bar.get("low", math.nan)),
                        c=float(bar.get("close", math.nan)),
                        v=int(bar.get("volume", 0) or 0),
                    )
                )
        out.sort(key=lambda b: (b["symbol"], b["ts_ist"]))
        return out

    # ---------------------------------------------------------------------- #
    # Futures OI
    # ---------------------------------------------------------------------- #

    def fetch_fut_oi(self, symbols: Iterable[str]) -> list[FutOiRow]:
        token_map: Dict[int, str] = {}
        for symbol in symbols:
            t = self._resolve_future_token(symbol)
            if t:
                token_map[t] = symbol
        if not token_map:
            return []
        quotes = self._retry(self._kite.quote, "fut_quote", list(token_map.keys()))
        now = now_ist().astimezone(self._tz)
        rows: list[FutOiRow] = []
        for tkn, sym in token_map.items():
            q = quotes.get(tkn) or quotes.get(str(tkn)) or {}
            price = q.get("last_price") or q.get("last_trade_price") or 0.0
            oi = q.get("oi") or q.get("open_interest") or 0
            rows.append(FutOiRow(symbol=sym, ts_ist=now, price=float(price), oi=int(oi), baseline_tag=None))
        rows.sort(key=lambda r: (r["symbol"], r["ts_ist"]))
        return rows

    # ---------------------------------------------------------------------- #
    # Option Chain with IV + Greeks
    # ---------------------------------------------------------------------- #

    def _atm_center(self, symbol: str) -> Optional[float]:
        idx_token = self._resolve_index_token(symbol)
        if not idx_token:
            return None
        q = self._retry(self._kite.quote, f"index_quote:{symbol}", [idx_token])
        q = q.get(idx_token) or q.get(str(idx_token)) or {}
        last = q.get("last_price") or q.get("last_trade_price")
        try:
            return float(last) if last is not None else None
        except Exception:
            return None

    def _expiry_dt_1530(self, exp: date | datetime) -> datetime:
        if isinstance(exp, datetime):
            return exp.astimezone(self._tz) if exp.tzinfo else exp.replace(tzinfo=self._tz)
        return datetime.combine(exp, dtime(15, 30)).replace(tzinfo=self._tz)

    def fetch_option_chain(self, symbol: str) -> list[OptionRow]:
        opts = [inst for inst in self._instrument_cache.values() if self._norm(symbol) in self._norm(inst.get("tradingsymbol", ""))]
        if not opts:
            return []
        S = self._atm_center(symbol)
        if not S:
            return []
        picks = [(o, float(o.get("strike") or 0), "CE" if str(o.get("tradingsymbol", "")).endswith("CE") else "PE") for o in opts]
        tokens = [int(p[0]["instrument_token"]) for p in picks]
        quotes = self._retry(self._kite.quote, f"opt_quote:{symbol}", tokens)
        now = now_ist().astimezone(self._tz)
        r = self._risk_free_rate
        qd = self._div_yield
        out: list[OptionRow] = []
        for inst, strike, side in picks:
            tkn = int(inst["instrument_token"])
            qrow = quotes.get(tkn) or quotes.get(str(tkn)) or {}
            last = qrow.get("last_price") or qrow.get("last_trade_price") or 0.0
            oi = qrow.get("oi") or qrow.get("open_interest") or 0
            vol = qrow.get("volume") or qrow.get("total_traded_volume") or 0
            iv_val = delta = gamma = theta = vega = None
            exp_raw = inst.get("expiry")
            if S and S > 0 and strike > 0 and float(last) > 0 and exp_raw:
                exp_dt = self._expiry_dt_1530(exp_raw)
                T = year_fraction(exp_dt, now.astimezone(timezone.utc))
                if T > 0:
                    iv_guess = implied_vol(float(last), float(S), float(strike), T, r, qd, side)
                    if iv_guess and iv_guess > 0:
                        iv_val = float(iv_guess)
                        d, g, th, v = bs_greeks(float(S), float(strike), T, r, qd, iv_val, side)
                        delta, gamma, theta, vega = float(d), float(g), float(th), float(v)
            out.append(
                OptionRow(
                    symbol=symbol,
                    ts_ist=now,
                    expiry=str(exp_raw),
                    strike=float(strike),
                    side=side,
                    ltp=float(last),
                    iv=iv_val,
                    oi=int(oi),
                    doi=None,
                    volume=int(vol),
                    delta=delta,
                    gamma=gamma,
                    theta=theta,
                    vega=vega,
                )
            )
        out.sort(key=lambda r: (r["symbol"], r["expiry"], r["strike"], r["side"], r["ts_ist"]))
        return out

    # ---------------------------------------------------------------------- #
    # Breadth & VIX
    # ---------------------------------------------------------------------- #

    def fetch_breadth(self) -> BreadthRow:
        raise NotImplementedError("Market breadth (adv/dec/unch) not available via Kite API.")

    def fetch_vix(self) -> VixRow:
        token = self._index_symbol_map.get("INDIAVIX")
        if not token:
            for inst in self._instrument_cache.values():
                name = self._norm(inst.get("tradingsymbol", "") or inst.get("name", ""))
                if "VIX" in name:
                    token = int(inst["instrument_token"])
                    self._index_symbol_map["INDIAVIX"] = token
                    break
        if not token:
            self._logger.warning("INDIA VIX token not found in instruments.")
            return VixRow(ts_ist=now_ist().astimezone(self._tz), value=0.0)
        q = self._retry(self._kite.quote, "vix_quote", [token])
        q = q.get(token) or q.get(str(token)) or {}
        val = q.get("last_price") or q.get("last_trade_price") or 0.0
        try:
            val = float(val)
        except Exception:
            val = 0.0
        return VixRow(ts_ist=now_ist().astimezone(self._tz), value=val)

    # ---------------------------------------------------------------------- #
    # ✅ Live LTP + WebSocket Support
    # ---------------------------------------------------------------------- #

    def fetch_ltp(self, symbols: list[str]) -> list[dict[str, Any]]:
        """Fetch latest LTP for given symbols. Returns [{'symbol','price','volume','ts'}]."""
        out: list[dict[str, Any]] = []
        token_map: dict[int, str] = {}
        for sym in symbols:
            token = self._resolve_index_token(sym)
            if token:
                token_map[token] = sym
        if not token_map:
            self._logger.warning("No valid tokens resolved for LTP fetch.")
            return out
        quotes = self._retry(self._kite.ltp, "ltp", list(token_map.keys()))
        ts = datetime.now(self._tz)
        for token, sym in token_map.items():
            q = quotes.get(token) or quotes.get(str(token)) or {}
            price = q.get("last_price") or q.get("last_trade_price")
            vol = q.get("volume") or q.get("total_traded_volume") or 0
            if price is not None:
                out.append({"symbol": sym, "price": float(price), "volume": int(vol), "ts": ts})
        return out

    def start_websocket(self, symbols: list[str], on_tick_callback) -> None:
        """Start KiteTicker stream and forward ticks to on_tick_callback(symbol, price, volume, ts)."""
        token_map: dict[int, str] = {}
        for sym in symbols:
            token = self._resolve_index_token(sym)
            if token:
                token_map[token] = sym
        if not token_map:
            self._logger.warning("No valid tokens for websocket subscription.")
            return

        ticker = KiteTicker(self._kite.api_key, self._kite.access_token)

        def on_ticks(ws, ticks):
            now = datetime.now(self._tz)
            for t in ticks:
                token = t.get("instrument_token")
                sym = token_map.get(token)
                if not sym:
                    continue
                price = t.get("last_price")
                vol = t.get("volume") or 0
                if price:
                    on_tick_callback(sym, float(price), int(vol), now)

        def on_connect(ws, response):
            self._logger.info("🔌 [KiteWS] Connected. Subscribing to %d tokens.", len(token_map))
            ws.subscribe(list(token_map.keys()))
            ws.set_mode(ws.MODE_LTP, list(token_map.keys()))

        def on_close(ws, code, reason):
            self._logger.warning("🔌 [KiteWS] Closed (%s): %s", code, reason)

        def on_error(ws, code, reason):
            self._logger.error("💥 [KiteWS] Error (%s): %s", code, reason)

        ticker.on_ticks = on_ticks
        ticker.on_connect = on_connect
        ticker.on_close = on_close
        ticker.on_error = on_error

        while True:
            try:
                self._logger.info("▶️ [KiteWS] Connecting WebSocket...")
                ticker.connect(threaded=True)
                while ticker.is_connected():  # ✅ safety: exit if disconnected
                    time.sleep(1)
            except Exception as e:
                self._logger.error("🔁 [KiteWS] Reconnecting after error: %s", e)
                time.sleep(5)

    # ---------------------------------------------------------------------- #
    # Diagnostics
    # ---------------------------------------------------------------------- #

    def get_rate_budget(self) -> dict[str, float | int]:
        return {"retries": self._max_attempts, "base_delay_sec": self._base_delay}
