from __future__ import annotations

import logging
from typing import Tuple

from pulsar_neuron.config.loader import load_defaults
from pulsar_neuron.db import upsert_breadth, upsert_vix
from pulsar_neuron.normalize import normalize_breadth, normalize_vix
from pulsar_neuron.providers import resolve_provider

logger = logging.getLogger(__name__)


def upsert_one(row: dict) -> None:
    upsert_breadth(
        {
            "ts_ist": row["ts_ist"],
            "adv": row["adv"],
            "dec": row["dec"],
            "unchanged": row["unchanged"],
        }
    )
    if "vix" in row:
        upsert_vix({"ts_ist": row["ts_ist"], "value": row["vix"]})


def run(mode: str | None = None) -> Tuple[dict | None, dict | None]:
    defaults = load_defaults()
    market_cfg = defaults.get("market", {})
    provider = resolve_provider(config=defaults, logger=logger)

    breadth_row = None
    if market_cfg.get("breadth", {}).get("enabled", True):
        raw_breadth = provider.fetch_breadth()
        breadth_row = normalize_breadth(raw_breadth)

    vix_row = None
    if market_cfg.get("vix", {}).get("enabled", True):
        raw_vix = provider.fetch_vix()
        vix_row = normalize_vix(raw_vix)

    if mode is not None:
        if breadth_row:
            payload = dict(breadth_row)
            if vix_row:
                payload["vix"] = vix_row["value"]
            upsert_one(payload)
        elif vix_row:
            upsert_vix(vix_row)
    else:
        if breadth_row:
            upsert_breadth(breadth_row)
        if vix_row:
            upsert_vix(vix_row)

    if breadth_row and vix_row:
        logger.info(
            "ADV:%s DEC:%s UNCH:%s • VIX=%.2f",
            breadth_row["adv"],
            breadth_row["dec"],
            breadth_row["unchanged"],
            vix_row["value"],
        )
    elif breadth_row:
        logger.info(
            "ADV:%s DEC:%s UNCH:%s",
            breadth_row["adv"],
            breadth_row["dec"],
            breadth_row["unchanged"],
        )
    elif vix_row:
        logger.info("VIX=%.2f", vix_row["value"])
    else:
        logger.info("Market job disabled (breadth & vix)")

    return breadth_row, vix_row


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    run()


if __name__ == "__main__":
    main()
