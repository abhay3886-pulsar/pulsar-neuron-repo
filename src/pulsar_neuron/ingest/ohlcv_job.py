from __future__ import annotations

import argparse
import logging
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Sequence

from pulsar_neuron.config.loader import load_markets
from pulsar_neuron.db import upsert_ohlcv
from pulsar_neuron.normalize import normalize_ohlcv
from pulsar_neuron.providers import resolve_provider
from pulsar_neuron.providers.market_provider import Timeframe

logger = logging.getLogger(__name__)


# ------------------------- parsing helpers -------------------------

def _parse_symbols(arg: str | None) -> list[str]:
    """
    --symbols can be:
      - None: load from markets config (prefer tokens, fallback to symbols)
      - path to a file: one symbol per line
      - comma-separated string
    """
    if not arg:
        markets = load_markets()
        tokens = (markets.get("tokens") or {})
        if tokens:
            return list(tokens.keys())
        fallback = markets.get("symbols") or []
        if not fallback:
            logger.warning("No symbols found in markets config (tokens/symbols empty).")
        return list(fallback)

    path = Path(arg)
    if path.exists():
        with path.open("r", encoding="utf-8") as fh:
            syms = [line.strip() for line in fh if line.strip()]
            return list(dict.fromkeys(syms))  # dedupe, keep order

    # CSV string
    syms = [item.strip() for item in arg.split(",") if item.strip()]
    return list(dict.fromkeys(syms))


def _parse_since(value: str | None) -> datetime | None:
    """
    Accepts ISO8601 (supports 'Z'). If naive, assumes UTC (consistent with most providers).
    """
    if not value:
        return None
    txt = value.strip().replace("Z", "+00:00")
    dt = datetime.fromisoformat(txt)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


# ------------------------- fetch helpers -------------------------

def _chunk(seq: Sequence[str], n: int) -> Iterable[list[str]]:
    for i in range(0, len(seq), n):
        yield list(seq[i:i + n])


def _fetch_ohlcv(symbols: Sequence[str], tf: Timeframe, since: datetime | None,
                 *, retries: int = 3, delay_s: float = 0.8, chunk_size: int = 150) -> list[dict]:
    """
    Fetch via configured provider with simple retries and chunking.
    """
    provider = resolve_provider(logger=logger)
    last_err: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            rows: list[dict] = []
            # Some providers accept large batches; chunk to avoid rate limits/timeouts.
            for chunk in _chunk(symbols, chunk_size):
                rows.extend(provider.fetch_ohlcv(chunk, tf, since))
            if rows:
                return rows
            logger.warning("Attempt %d: provider returned 0 bars.", attempt)
        except Exception as e:
            last_err = e
            logger.warning("Attempt %d: fetch_ohlcv failed: %s", attempt, e, exc_info=True)
        time.sleep(delay_s)
    if last_err:
        raise last_err
    return []


# ------------------------- job core -------------------------

def run(symbols: Sequence[str], tf: Timeframe, since: datetime | None = None) -> list[dict]:
    """
    Ingest OHLCV bars for given symbols/timeframe since (optional).
    Returns the normalized list of bar dicts (even if empty).
    """
    logger.info("Starting OHLCV job tf=%s symbols=%d since=%s", tf, len(symbols), since)

    if not symbols:
        logger.warning("No symbols provided; nothing to do.")
        return []

    raw = _fetch_ohlcv(symbols, tf, since)
    normalized = normalize_ohlcv(raw) if raw else []

    if normalized:
        upsert_ohlcv(normalized)
        counts = Counter(bar["symbol"] for bar in normalized)
        for symbol, count in counts.items():
            logger.info("Inserted %s bars for %s", count, symbol)
        logger.info("Total bars=%s", len(normalized))
    else:
        logger.info("No normalized OHLCV bars to upsert.")

    return normalized


# ------------------------- CLI -------------------------

def main(argv: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Ingest OHLCV bars from market provider")
    parser.add_argument("--tf", choices=["5m", "15m", "1d"], default="5m")
    parser.add_argument("--symbols", help="Comma separated symbols or path to file")
    parser.add_argument("--since", help="ISO8601 start timestamp (e.g., 2025-09-30T09:15:00Z)")
    args = parser.parse_args(argv)

    if not logging.getLogger().hasHandlers():
        logging.basicConfig(level=logging.INFO)

    symbols = _parse_symbols(args.symbols)
    since = _parse_since(args.since)

    # Cast the CLI string to the provider's Timeframe Literal
    tf: Timeframe = args.tf  # type: ignore[assignment]
    run(symbols, tf, since)


if __name__ == "__main__":
    main()
