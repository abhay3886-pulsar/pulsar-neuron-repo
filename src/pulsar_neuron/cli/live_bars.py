from __future__ import annotations

import signal
import sys
import threading
import time
from datetime import datetime, timezone
from typing import Dict, List

from pulsar_neuron.config.kite_auth import TokenWatcher, load_kite_creds
from pulsar_neuron.config.loader import load_config
from pulsar_neuron.ingest.bar_builder import BarBuilder
from pulsar_neuron.ingest.derive_tfs import derive_15m
from pulsar_neuron.db_legacy.ohlcv_repo import upsert_many

try:
    from kiteconnect import KiteTicker  # type: ignore
except Exception:  # pragma: no cover
    KiteTicker = None  # type: ignore


def _load_tokens() -> Dict[str, int]:
    markets = load_config("markets.yaml")
    tokens = markets.get("tokens") or {}
    if not tokens:
        raise RuntimeError("Token map missing in markets.yaml under 'tokens'.")
    return {str(symbol): int(token) for symbol, token in tokens.items()}


def main():
    creds = load_kite_creds()
    if not creds.get("api_key") or not creds.get("access_token"):
        raise RuntimeError(f"Invalid Kite creds: got keys {list(creds.keys())}")
    api_key, access_token = creds["api_key"], creds["access_token"]

    if KiteTicker is None:
        raise RuntimeError("kiteconnect is not installed. pip install kiteconnect")

    token_map = _load_tokens()
    symbols = list(token_map.keys())
    tokens = list(token_map.values())
    token_to_symbol: Dict[int, str] = {tok: sym for sym, tok in token_map.items()}

    builder = BarBuilder(symbols=symbols, tf="5m")
    lock = threading.Lock()
    recent_5m_buffer: Dict[str, List[dict]] = {s: [] for s in symbols}

    def on_ticks(ws, ticks):
        now_utc = datetime.now(timezone.utc)
        with lock:
            for t in ticks:
                token = t.get("instrument_token")
                price_raw = t.get("last_price") or t.get("last_trade_price")
                if not token or price_raw in (None, 0, 0.0):
                    continue
                symbol = token_to_symbol.get(token)
                if not symbol:
                    continue
                price = float(price_raw)
                vol = int(t.get("volume", 0) or 0)
                builder.on_tick(symbol, price, vol=vol, ts=now_utc)

    def on_connect(ws, response):
        ws.subscribe(tokens)
        ws.set_mode(ws.MODE_FULL, tokens)
        print("[live_bars] connected & subscribed")

    def on_close(ws, code, reason):
        print(f"[live_bars] socket closed: {code} {reason}", file=sys.stderr)

        # try quick reconnect (non-fatal)
        try:
            ws.connect(threaded=True)
        except Exception:
            pass

    kws = KiteTicker(api_key, access_token)
    kws.on_ticks = on_ticks
    kws.on_connect = on_connect
    kws.on_close = on_close

    stop = threading.Event()

    watcher = TokenWatcher()

    def token_watchdog():
        nonlocal kws
        while not stop.is_set():
            if watcher.wait_for_change() and not stop.is_set():
                print("[live_bars] token changed → reconnecting…")
                try:
                    kws.stop()
                except Exception:
                    pass
                new = load_kite_creds()
                new_api_key, new_access_token = new["api_key"], new["access_token"]
                kws = KiteTicker(new_api_key, new_access_token)
                kws.on_ticks = on_ticks
                kws.on_connect = on_connect
                kws.on_close = on_close
                kws.connect(threaded=True)

    tw = threading.Thread(target=token_watchdog, daemon=True)
    tw.start()

    def flusher():
        while not stop.is_set():
            time.sleep(1.0)
            with lock:
                completed = builder.maybe_close()
            if not completed:
                continue

            for b in completed:
                s = b["symbol"]
                recent_5m_buffer[s].append(b)
                if len(recent_5m_buffer[s]) > 12:
                    recent_5m_buffer[s] = recent_5m_buffer[s][-12:]

            # write 5m
            try:
                upsert_many(completed)
            except Exception as e:
                print(f"[live_bars] upsert_many(5m) failed: {e}", file=sys.stderr)

            # derive & write 15m
            derived_rows: List[dict] = []
            for s, buf in recent_5m_buffer.items():
                if len(buf) >= 3:
                    last_three = buf[-3:]
                    derived_rows.extend(derive_15m(last_three))
            if derived_rows:
                try:
                    upsert_many(derived_rows)
                except Exception as e:
                    print(f"[live_bars] upsert_many(15m) failed: {e}", file=sys.stderr)

    t = threading.Thread(target=flusher, daemon=True)
    t.start()

    def _shutdown(*_):
        stop.set()
        try:
            kws.stop()
        except Exception:
            pass
        t.join(timeout=2.0)
        tw.join(timeout=2.0)
        print("[live_bars] Shutdown complete.")
        sys.exit(0)

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    # ✅ run in threaded mode initially too (consistent with hot-reload reconnects)
    kws.connect(threaded=True)

    # keep main thread alive
    try:
        while not stop.is_set():
            time.sleep(0.5)
    except KeyboardInterrupt:
        _shutdown()