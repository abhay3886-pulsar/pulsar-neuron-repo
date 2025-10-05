from __future__ import annotations

import logging
import time
from datetime import date, time as dtime
from typing import Sequence

from pulsar_neuron.config.loader import load_markets
from pulsar_neuron.db import insert_or_update_fut_oi_baseline, upsert_fut_oi
from pulsar_neuron.normalize import normalize_fut_oi
from pulsar_neuron.providers import resolve_provider
from pulsar_neuron.timeutils import now_ist

logger = logging.getLogger(__name__)

# ---- Baseline window (IST) ---------------------------------------------------

BASELINE_START = dtime(hour=9, minute=20)
BASELINE_END = dtime(hour=9, minute=25)
_baseline_written: set[date] = set()

# ---- Helpers -----------------------------------------------------------------

def upsert_many(rows: list[dict]) -> None:  # legacy alias for tests
    upsert_fut_oi(rows)

def _default_symbols() -> list[str]:
    markets = load_markets()
    tokens = markets.get("tokens") or {}
    if tokens:
        return list(tokens.keys())
    return markets.get("symbols", [])

def _fetch_fut_oi(symbols: list[str], retries: int = 3, delay_s: float = 1.0) -> list[dict]:
    """Fetch futures OI data using the configured provider, with simple retry."""
    provider = resolve_provider(logger=logger)
    last_err = None
    for attempt in range(1, retries + 1):
        try:
            rows = provider.fetch_fut_oi(symbols)
            if rows:
                return rows
            logger.warning("Attempt %d: provider returned 0 rows.", attempt)
        except Exception as e:
            last_err = e
            logger.warning("Attempt %d: fetch_fut_oi failed: %s", attempt, e, exc_info=True)
        time.sleep(delay_s)
    if last_err:
        raise last_err
    return []

# ---- Main job ----------------------------------------------------------------

def run(symbols: Sequence[str] | str | None = None) -> list[dict]:
    if isinstance(symbols, str):  # legacy single-symbol mode
        symbols = None
    symbols = list(symbols or _default_symbols())
    if not symbols:
        logger.warning("Futures OI job: empty universe (no symbols).")
        return []

    logger.info("Starting Futures OI ingestion | symbols=%d", len(symbols))

    # 1. Fetch raw
    raw = _fetch_fut_oi(symbols)
    if not raw:
        logger.warning("Provider returned no raw OI data.")
        return []

    # 2. Normalize
    normalized = normalize_fut_oi(raw)
    if not normalized:
        logger.info("No valid normalized rows.")
        return []

    # 3. Upsert
    upsert_many(normalized)
    logger.info("Upserted fut_oi rows: %d", len(normalized))

    # 4. Handle baseline (only between 09:20–09:25 IST)
    now = now_ist()
    current_time = now.time()
    if BASELINE_START <= current_time <= BASELINE_END:
        trading_day = now.date()
        if trading_day not in _baseline_written:
            baseline_rows = [
                {
                    "symbol": r["symbol"],
                    "ts_ist": r["ts_ist"],
                    "price": r["price"],
                    "oi": r["oi"],
                    "baseline_tag": "open_baseline",
                }
                for r in normalized
                if r.get("price") is not None and r.get("oi") is not None
            ]
            if baseline_rows:
                insert_or_update_fut_oi_baseline(baseline_rows, trading_day, "open_baseline")
                _baseline_written.add(trading_day)
                logger.info(
                    "Baseline snapshot written | trading_day=%s | rows=%d",
                    trading_day, len(baseline_rows)
                )
            else:
                logger.info("Skipping baseline: no valid rows with price & oi.")
        else:
            logger.debug("Baseline already recorded for %s", trading_day)
    else:
        logger.debug("Outside baseline window (%s–%s).", BASELINE_START, BASELINE_END)

    return normalized

# ---- CLI entry ---------------------------------------------------------------

def main() -> None:
    if not logging.getLogger().hasHandlers():
        logging.basicConfig(level=logging.INFO)
    run()

if __name__ == "__main__":
    main()
