from __future__ import annotations

import argparse
import logging
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Sequence

from pulsar_neuron.config.loader import load_markets
from pulsar_neuron.db import upsert_ohlcv
from pulsar_neuron.normalize import normalize_ohlcv
from pulsar_neuron.providers import resolve_provider
from pulsar_neuron.providers.market_provider import Timeframe

logger = logging.getLogger(__name__)


def _parse_symbols(arg: str | None) -> list[str]:
    if not arg:
        markets = load_markets()
        tokens = markets.get("tokens") or {}
        return list(tokens.keys())
    path = Path(arg)
    if path.exists():
        with path.open("r", encoding="utf-8") as fh:
            return [line.strip() for line in fh if line.strip()]
    return [item.strip() for item in arg.split(",") if item.strip()]


def _parse_since(value: str | None) -> datetime | None:
    if not value:
        return None
    txt = value.replace("Z", "+00:00")
    return datetime.fromisoformat(txt)


def run(symbols: Sequence[str], tf: Timeframe, since: datetime | None = None) -> list[dict]:
    logger.info("Starting OHLCV job tf=%s symbols=%s since=%s", tf, symbols, since)
    provider = resolve_provider(logger=logger)
    raw = provider.fetch_ohlcv(symbols, tf, since)
    normalized = normalize_ohlcv(raw)
    if normalized:
        upsert_ohlcv(normalized)
    counts = Counter(bar["symbol"] for bar in normalized)
    for symbol, count in counts.items():
        logger.info("Inserted %s bars for %s", count, symbol)
    logger.info("Total bars=%s", len(normalized))
    return normalized


def main(argv: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Ingest OHLCV bars from market provider")
    parser.add_argument("--tf", choices=["5m", "15m", "1d"], default="5m")
    parser.add_argument("--symbols", help="Comma separated symbols or path to file")
    parser.add_argument("--since", help="ISO8601 start timestamp")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO)
    symbols = _parse_symbols(args.symbols)
    since = _parse_since(args.since)
    run(symbols, args.tf, since)


if __name__ == "__main__":
    main()
