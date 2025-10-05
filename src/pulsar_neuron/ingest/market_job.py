from __future__ import annotations

import datetime
import logging

from pulsar_neuron.db.market_repo import upsert_one

log = logging.getLogger(__name__)


def run(mode: str = "live") -> None:
    """Insert a synthetic market breadth / VIX snapshot."""

    now = datetime.datetime.now(datetime.timezone.utc)
    row = {
        "ts_ist": now,
        "adv": 1_200,
        "dec": 900,
        "unch": 100,
        "vix": 13.25,
    }
    upsert_one(row)
    log.info("✅ market_job: inserted breadth row at %s", now.isoformat())
