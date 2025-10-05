# src/pulsar_neuron/ingest/ohlcv_job.py
from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta
from typing import List, Dict
from zoneinfo import ZoneInfo

from pulsar_neuron.db import ohlcv_repo
from pulsar_neuron.normalize import normalize_ohlcv
from pulsar_neuron.providers.kite_provider import KiteMarketProvider

# BarBuilder (your tick->bar aggregator)
from pulsar_neuron.ingest.bar_builder import BarBuilder, _next_5m_end, _as_ist

IST = ZoneInfo("Asia/Kolkata")
LOG = logging.getLogger(__name__)

# Track symbols (env overrideable): e.g. "NIFTY 50,NIFTY BANK"
_DEFAULT_SYMBOLS = ["NIFTY 50", "NIFTY BANK"]
_SYMBOLS: List[str] = [
    s.strip() for s in os.getenv("PULSAR_SYMBOLS", ",".join(_DEFAULT_SYMBOLS)).split(",") if s.strip()
]

# Module-level singleton builder so state carries across scheduler ticks
_BUILDER: BarBuilder | None = None


def _now_ist() -> datetime:
    return datetime.now(IST).replace(microsecond=0)


def _ensure_builder() -> BarBuilder:
    global _BUILDER
    if _BUILDER is None:
        LOG.info("🧱 [ohlcv_job] Initializing BarBuilder for symbols: %s", ", ".join(_SYMBOLS))
        _BUILDER = BarBuilder(symbols=_SYMBOLS, tf="5m")
    return _BUILDER


def _ingest_from_ltp(provider: KiteMarketProvider) -> int:
    """
    Preferred fast path: poll latest LTP/volume for symbols,
    feed BarBuilder, close any due 5m bars, and upsert.
    """
    builder = _ensure_builder()

    # We expect provider.fetch_ltp(symbols) -> list[{"symbol","price","volume?" ,"ts?"}]
    if not hasattr(provider, "fetch_ltp"):
        raise AttributeError("KiteMarketProvider.fetch_ltp is not available")

    # Feed ticks
    rows: List[Dict] = provider.fetch_ltp(_SYMBOLS)  # type: ignore[attr-defined]
    for r in rows:
        sym = r["symbol"]
        px = float(r["price"])
        vol = r.get("volume")
        ts = r.get("ts")
        builder.on_tick(sym, px, vol, ts)

    # Close any completed bars (scheduler already runs +10s after bar close)
    closed = builder.maybe_close(_now_ist())
    if not closed:
        return 0

    normalized = normalize_ohlcv(closed)
    return ohlcv_repo.upsert_many(normalized)


def _ingest_from_history(provider: KiteMarketProvider) -> int:
    """
    Fallback path: fetch already-closed 5m bars directly via historical API.
    Uses repo.get_max_ts to only pull what’s missing; safe to call repeatedly.
    """
    total = 0
    now = _now_ist()

    for sym in _SYMBOLS:
        # Find where to resume for this symbol/timeframe
        last_ts = ohlcv_repo.get_max_ts(sym, "5m") if hasattr(ohlcv_repo, "get_max_ts") else None
        # If first run, backfill a couple of days to seed DB
        since = (last_ts or (now - timedelta(days=3)))
        # Guard: ensure tz-aware IST
        since = since.astimezone(IST) if since.tzinfo else since.replace(tzinfo=IST)

        bars = provider.fetch_ohlcv([sym], "5m", since=since)
        if not bars:
            continue

        # Filter out the bar that is still in-progress if scheduler ran early:
        # Only keep bars whose ts_ist < next 5m boundary relative to now.
        boundary = _next_5m_end(now)
        materialized = [b for b in bars if _as_ist(b["ts_ist"]) < boundary]

        normalized = normalize_ohlcv(materialized)
        total += ohlcv_repo.upsert_many(normalized)

    return total


def run() -> int:
    """
    Entry used by the scheduler every minute (with +10s offset after 5m close).
    Tries LTP->BarBuilder path; if not available, falls back to historical bars.
    """
    LOG.info("📈 [ohlcv_job] Starting OHLCV ingest (5m, IST)")
    try:
        provider = KiteMarketProvider()

        # Try live LTP/ticks + BarBuilder
        try:
            written = _ingest_from_ltp(provider)
            if written:
                LOG.info("✅ [ohlcv_job] Stored %d bars (ltp+builder)", written)
                return written
        except AttributeError:
            LOG.debug("fetch_ltp not available on provider; falling back to historical")

        # Fallback to historical bars (idempotent backfill)
        written = _ingest_from_history(provider)
        LOG.info("✅ [ohlcv_job] Stored %d bars (historical)", written)
        return written

    except Exception as e:
        LOG.exception("❌ [ohlcv_job] Error: %s", e)
        return 0


def main():
    logging.basicConfig(level=logging.INFO)
    run()


if __name__ == "__main__":  # pragma: no cover
    main()
