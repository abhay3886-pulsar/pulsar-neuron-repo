from __future__ import annotations

import logging
import time
from collections import defaultdict
from statistics import median
from typing import Dict, Iterable, List, Sequence, Tuple

from pulsar_neuron.config.loader import load_defaults
from pulsar_neuron.db import upsert_option_chain
from pulsar_neuron.normalize import normalize_option_chain
from pulsar_neuron.providers import resolve_provider

logger = logging.getLogger(__name__)


# ---------------------------- legacy alias ----------------------------

def upsert_many(rows: list[dict]) -> None:  # legacy alias
    upsert_option_chain(rows)


# ---------------------------- config helpers ----------------------------

def _default_symbols() -> list[str]:
    defaults = load_defaults()
    market_cfg = defaults.get("market", {}) if isinstance(defaults, dict) else {}
    options_cfg = market_cfg.get("options", {}) if isinstance(market_cfg, dict) else {}
    symbols = options_cfg.get("index_symbols")
    if symbols:
        return list(symbols)
    # sensible default indices
    return ["NIFTY 50", "NIFTY BANK"]


def _options_limits() -> Tuple[int | None, int | None]:
    """
    Read optional thinning knobs:
      - expiries_limit: keep only the nearest N expiries (by date asc).
      - max_strikes_per_side: keep up to N strikes per (symbol, expiry, side), chosen near median strike.
    Return (expiries_limit, max_strikes_per_side) or (None, None) if not configured.
    """
    defaults = load_defaults()
    market_cfg = defaults.get("market", {}) if isinstance(defaults, dict) else {}
    options_cfg = market_cfg.get("options", {}) if isinstance(market_cfg, dict) else {}

    expiries_limit = options_cfg.get("expiries_limit")
    max_strikes_per_side = options_cfg.get("max_strikes_per_side")
    try:
        expiries_limit = int(expiries_limit) if expiries_limit is not None else None
    except Exception:
        expiries_limit = None
    try:
        max_strikes_per_side = int(max_strikes_per_side) if max_strikes_per_side is not None else None
    except Exception:
        max_strikes_per_side = None
    return expiries_limit, max_strikes_per_side


# ---------------------------- thinning logic ----------------------------

def _thin_by_expiry(rows: List[dict], expiries_limit: int | None) -> List[dict]:
    if not expiries_limit or expiries_limit <= 0:
        return rows
    # keep nearest N expiries by ascending date
    by_expiry: Dict[object, List[dict]] = defaultdict(list)
    for r in rows:
        by_expiry[r["expiry"]].append(r)
    expiries_sorted = sorted(by_expiry.keys())
    keep_set = set(expiries_sorted[:expiries_limit])
    return [r for r in rows if r["expiry"] in keep_set]


def _thin_by_strikes_per_side(rows: List[dict], max_strikes_per_side: int | None) -> List[dict]:
    if not max_strikes_per_side or max_strikes_per_side <= 0:
        return rows

    # group by (symbol, expiry, side)
    grouped: Dict[Tuple[str, object, str], List[dict]] = defaultdict(list)
    for r in rows:
        grouped[(r["symbol"], r["expiry"], r["side"])].append(r)

    out: List[dict] = []
    for key, items in grouped.items():
        # strikes present?
        strikes = [float(it["strike"]) for it in items]
        if not strikes:
            out.extend(items)
            continue

        # choose strikes closest to median strike (neutral heuristic when ATM is unknown)
        med = median(strikes)
        # sort by distance to median, then by strike
        sorted_items = sorted(items, key=lambda it: (abs(float(it["strike"]) - med), float(it["strike"])))
        out.extend(sorted_items[:max_strikes_per_side])
    return out


# ---------------------------- provider fetch with retry ----------------------------

def _fetch_chain_for_symbol(symbol: str, provider, retries: int = 3, delay_s: float = 0.8) -> List[dict]:
    last_err: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            raw = provider.fetch_option_chain(symbol)
            if not raw:
                logger.warning("%s: attempt %d provider returned empty chain.", symbol, attempt)
                time.sleep(delay_s)
                continue
            normalized = normalize_option_chain(raw)
            return normalized or []
        except Exception as e:
            last_err = e
            logger.warning("%s: attempt %d fetch/normalize failed: %s", symbol, attempt, e, exc_info=True)
            time.sleep(delay_s)
    if last_err:
        logger.error("%s: giving up after %d attempts.", symbol, retries)
    return []


# ---------------------------- job core ----------------------------

def run(symbols: Sequence[str] | str | None = None) -> list[dict]:
    if isinstance(symbols, str):  # legacy mode parameter
        symbols = None
    symbols = list(symbols or _default_symbols())
    if not symbols:
        logger.warning("Options job: empty symbol list.")
        return []

    logger.info("Starting options job | symbols=%s", symbols)
    provider = resolve_provider(logger=logger)

    expiries_limit, max_strikes_per_side = _options_limits()

    all_rows: List[dict] = []
    total_raw = 0
    total_thin = 0

    for symbol in symbols:
        rows = _fetch_chain_for_symbol(symbol, provider)
        if not rows:
            logger.warning("No option chain rows for %s", symbol)
            continue

        total_raw += len(rows)

        # Optional thinning
        rows = _thin_by_expiry(rows, expiries_limit)
        rows = _thin_by_strikes_per_side(rows, max_strikes_per_side)
        total_thin += len(rows)

        # Stats for this symbol
        expiries = {r["expiry"] for r in rows}
        strikes = {r["strike"] for r in rows}
        sides = {r["side"] for r in rows}
        logger.info(
            "%s: captured %s expiries × %s strikes × %s sides (rows=%d)",
            symbol, len(expiries), len(strikes), len(sides), len(rows)
        )

        all_rows.extend(rows)

    if all_rows:
        upsert_many(all_rows)
        logger.info(
            "Upserted option rows: %d (raw=%d → kept=%d; expiries_limit=%s, max_strikes_per_side=%s)",
            len(all_rows), total_raw, total_thin, expiries_limit, max_strikes_per_side
        )
    else:
        logger.info("No option rows to upsert.")

    logger.info("Total option rows=%s", len(all_rows))
    return all_rows


def main() -> None:
    if not logging.getLogger().hasHandlers():
        logging.basicConfig(level=logging.INFO)
    run()


if __name__ == "__main__":
    main()
