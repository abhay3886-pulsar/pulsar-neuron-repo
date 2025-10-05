from __future__ import annotations

import logging
import time
from typing import Callable, Dict, Optional, Tuple

from pulsar_neuron.config.loader import load_defaults
from pulsar_neuron.db import upsert_breadth, upsert_vix
from pulsar_neuron.normalize import normalize_breadth, normalize_vix
from pulsar_neuron.providers import resolve_provider

logger = logging.getLogger(__name__)


# --------------------------- helpers -----------------------------------------

def _retry(fn: Callable[[], Dict], what: str, retries: int = 3, delay_s: float = 0.8) -> Optional[Dict]:
    last_err: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            res = fn()
            if res:
                return res
            logger.warning("%s attempt %d: provider returned empty payload.", what, attempt)
        except Exception as e:
            last_err = e
            logger.warning("%s attempt %d failed: %s", what, attempt, e, exc_info=True)
        time.sleep(delay_s)
    if last_err:
        logger.error("Giving up %s after %d attempts.", what, retries)
    return None


def _normalize_unch_key(row: Dict) -> Dict:
    """Accept both 'unch' and 'unchanged' from the normalizer/providers; write 'unch' to DB."""
    if "unch" in row:
        return row
    if "unchanged" in row:
        r = dict(row)
        r["unch"] = r.pop("unchanged")
        return r
    # If neither present, keep as-is (DB upsert will likely fail; better to log)
    logger.warning("Breadth row missing 'unch'/'unchanged' keys: %s", row)
    return row


# --------------------------- DB convenience ----------------------------------

def upsert_one(row: dict) -> None:
    """
    Atomic upsert that writes breadth and, if present, vix at the same timestamp.
    Accepts 'unch' or 'unchanged' in the input; coerces to 'unch'.
    """
    row = _normalize_unch_key(row)
    breadth_payload = {
        "ts_ist": row["ts_ist"],
        "adv": row["adv"],
        "dec": row["dec"],
        "unch": row["unch"],
    }
    upsert_breadth(breadth_payload)

    if "vix" in row and row["vix"] is not None:
        upsert_vix({"ts_ist": row["ts_ist"], "value": row["vix"]})


# --------------------------- job core ----------------------------------------

def run(mode: str | None = None) -> Tuple[dict | None, dict | None]:
    """
    If `mode` is not None, behaves like an atomic write path (breadth+vix via `upsert_one`),
    otherwise writes each stream independently (legacy default).
    Returns (breadth_row, vix_row) — both are normalized dicts or None.
    """
    defaults = load_defaults()
    market_cfg = defaults.get("market", {}) if isinstance(defaults, dict) else {}

    provider = resolve_provider(config=defaults, logger=logger)

    # Feature flags
    breadth_enabled = bool(market_cfg.get("breadth", {}).get("enabled", True))
    vix_enabled = bool(market_cfg.get("vix", {}).get("enabled", True))

    # 1) Fetch & normalize (independently, with retries)
    breadth_row: Optional[dict] = None
    if breadth_enabled:
        raw_breadth = _retry(lambda: provider.fetch_breadth(), "fetch_breadth")  # type: ignore[arg-type]
        if raw_breadth:
            try:
                breadth_row = normalize_breadth(raw_breadth)
            except Exception as e:
                logger.warning("normalize_breadth failed: %s", e, exc_info=True)
                breadth_row = None
    else:
        logger.info("Breadth disabled via config.")

    vix_row: Optional[dict] = None
    if vix_enabled:
        raw_vix = _retry(lambda: provider.fetch_vix(), "fetch_vix")  # type: ignore[arg-type]
        if raw_vix:
            try:
                vix_row = normalize_vix(raw_vix)
            except Exception as e:
                logger.warning("normalize_vix failed: %s", e, exc_info=True)
                vix_row = None
    else:
        logger.info("VIX disabled via config.")

    # 2) Upserts
    if mode is not None:
        # Atomic-style path: if breadth exists, include vix (if any) in the same call
        if breadth_row:
            payload = dict(_normalize_unch_key(breadth_row))
            if vix_row and vix_row.get("value") is not None:
                payload["vix"] = vix_row["value"]
            upsert_one(payload)
        elif vix_row:
            upsert_vix(vix_row)
    else:
        # Legacy independent path
        if breadth_row:
            upsert_breadth(_normalize_unch_key(breadth_row))
        if vix_row:
            upsert_vix(vix_row)

    # 3) Logs
    if breadth_row and vix_row:
        b = _normalize_unch_key(breadth_row)
        logger.info("ADV:%s DEC:%s UNCH:%s • VIX=%.2f", b["adv"], b["dec"], b["unch"], float(vix_row["value"]))
    elif breadth_row:
        b = _normalize_unch_key(breadth_row)
        logger.info("ADV:%s DEC:%s UNCH:%s", b["adv"], b["dec"], b["unch"])
    elif vix_row:
        logger.info("VIX=%.2f", float(vix_row["value"]))
    else:
        logger.info("Market job produced no rows (disabled or provider returned empty).")

    return breadth_row, vix_row


def main() -> None:
    if not logging.getLogger().hasHandlers():
        logging.basicConfig(level=logging.INFO)
    run()


if __name__ == "__main__":
    main()
