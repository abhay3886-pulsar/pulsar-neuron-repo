from __future__ import annotations

import logging
from datetime import date, time
from typing import Sequence

from pulsar_neuron.config.loader import load_markets
from pulsar_neuron.db import insert_or_update_fut_oi_baseline, upsert_fut_oi
from pulsar_neuron.normalize import normalize_fut_oi
from pulsar_neuron.providers import resolve_provider
from pulsar_neuron.timeutils import now_ist

logger = logging.getLogger(__name__)


BASELINE_START = time(hour=9, minute=20)
BASELINE_END = time(hour=9, minute=25)
_baseline_written: set[date] = set()


def upsert_many(rows: list[dict]) -> None:  # legacy alias for tests
    upsert_fut_oi(rows)


def _default_symbols() -> list[str]:
    markets = load_markets()
    tokens = markets.get("tokens") or {}
    if tokens:
        return list(tokens.keys())
    return markets.get("symbols", [])


def run(symbols: Sequence[str] | str | None = None) -> list[dict]:
    if isinstance(symbols, str):  # legacy mode parameter
        symbols = None
    symbols = list(symbols or _default_symbols())
    logger.info("Starting futures OI job symbols=%s", symbols)
    provider = resolve_provider(logger=logger)
    raw = provider.fetch_fut_oi(symbols)
    normalized = normalize_fut_oi(raw)
    if not normalized:
        logger.info("No futures OI rows to upsert")
        return []
    upsert_many(normalized)

    now = now_ist()
    current_time = now.time()
    if BASELINE_START <= current_time <= BASELINE_END:
        trading_day = now.date()
        if trading_day in _baseline_written:
            logger.debug("Baseline already recorded for %s", trading_day)
            return normalized
        baseline_rows = []
        for row in normalized:
            baseline_rows.append(
                {
                    "symbol": row["symbol"],
                    "ts_ist": row["ts_ist"],
                    "price": row["price"],
                    "oi": row["oi"],
                    "baseline_tag": "open_baseline",
                }
            )
        insert_or_update_fut_oi_baseline(baseline_rows, trading_day, "open_baseline")
        logger.info("Baseline snapshot written for trading day %s", trading_day)
        _baseline_written.add(trading_day)
    else:
        logger.debug("Outside baseline window (%s - %s)", BASELINE_START, BASELINE_END)
    return normalized


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    run()


if __name__ == "__main__":
    main()
