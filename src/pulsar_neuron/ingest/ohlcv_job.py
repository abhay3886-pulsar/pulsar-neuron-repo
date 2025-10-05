from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta, time
from typing import List, Dict
from zoneinfo import ZoneInfo

from pulsar_neuron.db import ohlcv_repo
from pulsar_neuron.normalize import normalize_ohlcv
from pulsar_neuron.providers.kite_provider import KiteMarketProvider
from pulsar_neuron.ingest.bar_builder import BarBuilder, _next_5m_end, _as_ist
from pulsar_neuron.ingest.ohlcv_postprocess import postprocess_and_store  # 🆕 new aggregator

# -----------------------------------------------------------------------------
# Global setup
# -----------------------------------------------------------------------------
IST = ZoneInfo("Asia/Kolkata")
SESSION_END = time(15, 30)
LOG = logging.getLogger(__name__)

_DEFAULT_SYMBOLS = ["NIFTY 50", "NIFTY BANK"]
_SYMBOLS: List[str] = [
    s.strip()
    for s in os.getenv("PULSAR_SYMBOLS", ",".join(_DEFAULT_SYMBOLS)).split(",")
    if s.strip()
]

_BUILDER: BarBuilder | None = None
_WS_FLAG_PATH = "/tmp/pulsar_ws_active"  # marker used by ohlcv_ws_daemon.py


# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------
def _now_ist() -> datetime:
    return datetime.now(IST).replace(microsecond=0)


def _ensure_builder() -> BarBuilder:
    global _BUILDER
    if _BUILDER is None:
        LOG.info("🧱 [ohlcv_job] Initializing BarBuilder for: %s", ", ".join(_SYMBOLS))
        _BUILDER = BarBuilder(symbols=_SYMBOLS, tf="5m")
    return _BUILDER


def _reset_if_session_over(now: datetime) -> None:
    global _BUILDER
    if _BUILDER and now.time() >= SESSION_END:
        LOG.info("🌇 [ohlcv_job] Session ended; resetting builder state.")
        _BUILDER = None


def _ws_active() -> bool:
    """Skip job if WS daemon is running (creates marker file)."""
    try:
        return os.path.exists(_WS_FLAG_PATH)
    except Exception:
        return False


# -----------------------------------------------------------------------------
# Core ingestion
# -----------------------------------------------------------------------------
def _ingest_from_ltp(provider: KiteMarketProvider) -> int:
    builder = _ensure_builder()
    rows: List[Dict] = provider.fetch_ltp(_SYMBOLS)
    for r in rows:
        builder.on_tick(r["symbol"], float(r["price"]), r.get("volume"), r.get("ts"))

    closed = builder.maybe_close(_now_ist())
    if not closed:
        LOG.debug("⌛ [ohlcv_job] No 5m bar closed this tick.")
        return 0

    normalized = normalize_ohlcv(closed)
    written = ohlcv_repo.upsert_many(normalized)
    postprocess_and_store(closed)  # 🆕 build 15m/1h/1d bars
    LOG.info("✅ [ohlcv_job] Stored %d bars (ltp+builder)", written)
    return written


def _ingest_from_history(provider: KiteMarketProvider) -> int:
    total = 0
    now = _now_ist()
    for sym in _SYMBOLS:
        last_ts = ohlcv_repo.get_max_ts(sym, "5m") if hasattr(ohlcv_repo, "get_max_ts") else None
        since = (last_ts or (now - timedelta(days=3)))
        since = since.astimezone(IST) if since.tzinfo else since.replace(tzinfo=IST)

        bars = provider.fetch_ohlcv([sym], "5m", since=since)
        if not bars:
            continue

        boundary = _next_5m_end(now)
        materialized = [b for b in bars if _as_ist(b["ts_ist"]) < boundary]

        normalized = normalize_ohlcv(materialized)
        written = ohlcv_repo.upsert_many(normalized)
        if written:
            postprocess_and_store(materialized)
        total += written

    if total:
        LOG.info("✅ [ohlcv_job] Stored %d bars (historical)", total)
    else:
        LOG.info("ℹ️ [ohlcv_job] No new historical bars found.")
    return total


# -----------------------------------------------------------------------------
# Entrypoint
# -----------------------------------------------------------------------------
def run() -> int:
    now = _now_ist()
    LOG.info("📈 [ohlcv_job] Tick start at %s IST", now.strftime("%H:%M:%S"))

    if _ws_active():
        LOG.info("🟡 [ohlcv_job] Skipped — WS daemon active.")
        return 0

    try:
        provider = KiteMarketProvider()
        try:
            written = _ingest_from_ltp(provider)
            if written == 0:
                LOG.debug("💤 [ohlcv_job] No bars closed this run.")
            return written
        except AttributeError:
            LOG.warning("fetch_ltp not available; using historical fallback.")
            return _ingest_from_history(provider)
    except Exception as e:
        LOG.exception("❌ [ohlcv_job] Error: %s", e)
        return 0
    finally:
        _reset_if_session_over(now)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    run()


if __name__ == "__main__":  # pragma: no cover
    main()
