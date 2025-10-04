from __future__ import annotations

import json
import os
import signal
import sys
import threading
import time
from datetime import datetime, timezone
from typing import Dict, List

from pulsar_neuron.ingest.bar_builder import BarBuilder, IST
from pulsar_neuron.ingest.derive_tfs import derive_15m
from pulsar_neuron.db.ohlcv_repo import upsert_many

try:
    from kiteconnect import KiteTicker  # type: ignore
except Exception:  # pragma: no cover
    KiteTicker = None  # type: ignore


def _load_tokens() -> Dict[str, int]:
    raw = os.getenv("KITE_TOKENS_JSON", "")
    if not raw:
        raise RuntimeError(
            "KITE_TOKENS_JSON not set. Example: '{\"NIFTY 50\":256265, \"NIFTY BANK\":260105}'"
        )
    m = json.loads(raw)
    return {k: int(v) for k, v in m.items()}


def main():
    api_key = os.getenv("KITE_API_KEY")
    access_token = os.getenv("KITE_ACCESS_TOKEN")
    if not api_key or not access_token:
        raise RuntimeError("Set KITE_API_KEY and KITE_ACCESS_TOKEN.")
    if KiteTicker is None:
        raise RuntimeError("kiteconnect is not installed. pip install kiteconnect")

    token_map = _load_tokens()
    symbols = list(token_map.keys())
    tokens = list(token_map.values())

    builder = BarBuilder(symbols=symbols, tf="5m")
    lock = threading.Lock()
    recent_5m_buffer: Dict[str, List[dict]] = {s: [] for s in symbols}

    def on_ticks(ws, ticks):
        with lock:
            for t in ticks:
                token = t.get("instrument_token")
                price = float(t.get("last_price") or t.get("last_trade_price") or 0.0)
                if not token or not price:
                    continue
                symbol = next((s for s, tok in token_map.items() if tok == token), None)
                if not symbol:
                    continue
                vol = int(t.get("volume", 0))
                builder.on_tick(symbol, price, vol=vol, ts=datetime.now(timezone.utc))

    def on_connect(ws, response):
        ws.subscribe(tokens)
        ws.set_mode(ws.MODE_FULL, tokens)

    def on_close(ws, code, reason):
        print(f"[live_bars] socket closed: {code} {reason}", file=sys.stderr)

    kws = KiteTicker(api_key, access_token)
    kws.on_ticks = on_ticks
    kws.on_connect = on_connect
    kws.on_close = on_close

    stop = threading.Event()

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

            upsert_many(completed)

            derived_rows: List[dict] = []
            for s, buf in recent_5m_buffer.items():
                if len(buf) >= 3:
                    last_three = buf[-3:]
                    derived_rows.extend(derive_15m(last_three))
            if derived_rows:
                upsert_many(derived_rows)

    t = threading.Thread(target=flusher, daemon=True)
    t.start()

    def _shutdown(*_):
        stop.set()
        try:
            kws.stop()
        except Exception:
            pass
        t.join(timeout=2.0)
        print("[live_bars] Shutdown complete.")
        sys.exit(0)

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    kws.connect(threaded=False)


if __name__ == "__main__":
    main()
