"""CLI to run a live 5-minute bar builder using KiteTicker."""
from __future__ import annotations

import json
import os
import signal
import sys
import threading
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional

from pulsar_neuron.ingest.bar_builder import IST, SESSION_START, BarBuilder
from pulsar_neuron.ingest.derive_tfs import derive_15m
from pulsar_neuron.db.ohlcv_repo import upsert_many

try:
    from kiteconnect import KiteTicker
except Exception:  # pragma: no cover - optional dependency
    KiteTicker = None  # type: ignore[misc]


def _load_tokens() -> Dict[str, int]:
    raw = os.getenv("KITE_TOKENS_JSON", "")
    if not raw:
        raise RuntimeError(
            "KITE_TOKENS_JSON env missing. Example: '{\"NIFTY 50\":256265, \"NIFTY BANK\":260105}'"
        )
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as exc:  # pragma: no cover - validation
        raise RuntimeError("KITE_TOKENS_JSON must be valid JSON") from exc

    return {str(symbol): int(token) for symbol, token in payload.items()}


def _minutes_since_session_start(ts_ist: datetime) -> int:
    session_start = ts_ist.replace(
        hour=SESSION_START.hour,
        minute=SESSION_START.minute,
        second=0,
        microsecond=0,
    )
    return int((ts_ist - session_start).total_seconds() // 60)


def main() -> None:
    api_key = os.getenv("KITE_API_KEY")
    access_token = os.getenv("KITE_ACCESS_TOKEN")

    if not api_key or not access_token:
        raise RuntimeError("Set KITE_API_KEY and KITE_ACCESS_TOKEN in env")

    if KiteTicker is None:
        raise RuntimeError("kiteconnect not installed. Install with 'pip install kiteconnect'")

    token_map = _load_tokens()
    if not token_map:
        raise RuntimeError("No instruments provided in KITE_TOKENS_JSON")

    symbols = list(token_map.keys())
    tokens = list(token_map.values())
    token_to_symbol = {token: symbol for symbol, token in token_map.items()}

    builder = BarBuilder(symbols=symbols, tf="5m")
    lock = threading.Lock()
    recent_5m: Dict[str, List[dict]] = {symbol: [] for symbol in symbols}
    last_15m_upsert: Dict[str, datetime] = {symbol: datetime.min.replace(tzinfo=IST) for symbol in symbols}

    def on_ticks(_ws, ticks: List[Dict]) -> None:  # pragma: no cover - network callback
        now = datetime.now(timezone.utc)
        with lock:
            for tick in ticks:
                token = int(tick.get("instrument_token", 0))
                symbol = token_to_symbol.get(token)
                if not symbol:
                    continue

                price = float(tick.get("last_price") or tick.get("last_trade_price") or 0.0)
                if not price:
                    continue

                volume: Optional[int]
                if "volume" in tick and tick["volume"] is not None:
                    volume = int(tick["volume"])
                else:
                    volume = None

                builder.on_tick(symbol=symbol, price=price, vol=volume, ts=now)

    def on_connect(ws, _response) -> None:  # pragma: no cover - network callback
        ws.subscribe(tokens)
        ws.set_mode(ws.MODE_FULL, tokens)

    def on_close(_ws, code, reason) -> None:  # pragma: no cover - network callback
        print(f"WebSocket closed: {code} {reason}", file=sys.stderr)

    ticker = KiteTicker(api_key, access_token)
    ticker.on_ticks = on_ticks
    ticker.on_connect = on_connect
    ticker.on_close = on_close

    stop_event = threading.Event()

    def flusher() -> None:
        while not stop_event.is_set():
            time.sleep(1)
            with lock:
                completed = builder.maybe_close()

            if not completed:
                continue

            upsert_many(completed)

            for bar in completed:
                symbol = bar["symbol"]
                recent_5m[symbol].append(bar)
                if len(recent_5m[symbol]) > 12:
                    recent_5m[symbol] = recent_5m[symbol][-12:]

            derived_rows: List[dict] = []
            for symbol, buf in recent_5m.items():
                if len(buf) < 3:
                    continue

                last_three = buf[-3:]
                last_ts: datetime = last_three[-1]["ts_ist"].astimezone(IST)
                minutes_since = _minutes_since_session_start(last_ts)
                if minutes_since % 15 != 0:
                    continue

                if last_three[-1]["ts_ist"] <= last_15m_upsert[symbol]:
                    continue

                derived = derive_15m(last_three)
                if not derived:
                    continue

                derived_rows.extend(derived)
                last_15m_upsert[symbol] = last_three[-1]["ts_ist"]

            if derived_rows:
                upsert_many(derived_rows)

    thread = threading.Thread(target=flusher, daemon=True)
    thread.start()

    def shutdown(*_args) -> None:
        stop_event.set()
        try:
            ticker.stop()
        except Exception:  # pragma: no cover - defensive
            pass
        thread.join(timeout=2)
        print("Shutdown complete.")
        sys.exit(0)

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    ticker.connect(threaded=False)


if __name__ == "__main__":
    main()
