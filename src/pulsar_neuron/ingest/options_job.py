from __future__ import annotations

import logging
from typing import Sequence

from pulsar_neuron.config.loader import load_defaults
from pulsar_neuron.db import upsert_option_chain
from pulsar_neuron.normalize import normalize_option_chain
from pulsar_neuron.providers import resolve_provider

logger = logging.getLogger(__name__)


def upsert_many(rows: list[dict]) -> None:  # legacy alias
    upsert_option_chain(rows)


def _default_symbols() -> list[str]:
    defaults = load_defaults()
    market_cfg = defaults.get("market", {})
    options_cfg = market_cfg.get("options", {})
    symbols = options_cfg.get("index_symbols")
    if symbols:
        return list(symbols)
    return ["NIFTY 50", "NIFTY BANK"]


def run(symbols: Sequence[str] | str | None = None) -> list[dict]:
    if isinstance(symbols, str):  # legacy mode parameter
        symbols = None
    symbols = list(symbols or _default_symbols())
    logger.info("Starting options job symbols=%s", symbols)
    provider = resolve_provider(logger=logger)
    all_rows = []
    for symbol in symbols:
        raw = provider.fetch_option_chain(symbol)
        normalized = normalize_option_chain(raw)
        if not normalized:
            logger.warning("No option chain rows for %s", symbol)
            continue
        expiries = {row["expiry"] for row in normalized}
        strikes = {row["strike"] for row in normalized}
        sides = {row["side"] for row in normalized}
        logger.info(
            "%s: captured %s expiries × %s strikes × %s sides",
            symbol,
            len(expiries),
            len(strikes),
            len(sides),
        )
        all_rows.extend(normalized)
    if all_rows:
        upsert_many(all_rows)
    logger.info("Total option rows=%s", len(all_rows))
    return all_rows


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    run()


if __name__ == "__main__":
    main()
