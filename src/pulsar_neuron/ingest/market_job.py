# src/pulsar_neuron/ingest/market_job.py
from __future__ import annotations
import logging
from typing import Optional
from zoneinfo import ZoneInfo
from datetime import datetime

from pulsar_neuron.providers.kite_provider import KiteMarketProvider
from pulsar_neuron.db import market_repo

IST = ZoneInfo("Asia/Kolkata")
LOG = logging.getLogger(__name__)

def _now_ist() -> datetime:
    return datetime.now(IST).replace(microsecond=0)

def run() -> int:
    LOG.info("🌡️ [market_job] Starting market breadth + VIX ingest")
    try:
        provider = KiteMarketProvider()

        # Try breadth if provider supports it (optional)
        adv = dec = unch = 0
        ts_breadth: Optional[datetime] = None
        if hasattr(provider, "fetch_breadth"):
            try:
                b = provider.fetch_breadth()  # expects dict with adv/dec/unch/ts_ist
                adv = int(b.get("adv", 0) or 0)
                dec = int(b.get("dec", 0) or 0)
                unch = int(b.get("unchanged", b.get("unch", 0)) or 0)
                ts_breadth = b.get("ts_ist")
            except Exception:
                LOG.debug("breadth fetch not available or failed; falling back to zeros", exc_info=True)

        # VIX is required
        v = provider.fetch_vix()  # expects dict with {ts_ist, value}
        vix_val = float(v.get("value", 0.0) or 0.0)
        ts_vix = v.get("ts_ist")

        # pick a timestamp (prefer VIX ts, else breadth ts, else now IST)
        ts_ist = ts_vix or ts_breadth or _now_ist()

        row = {
            "ts_ist": ts_ist,
            "adv": adv,
            "dec": dec,
            "unch": unch,
            "vix": vix_val,
        }
        written = market_repo.upsert_one(row)
        LOG.info("✅ [market_job] Stored breadth(VIX=%.2f, adv=%d, dec=%d, unch=%d) @ %s",
                 vix_val, adv, dec, unch, ts_ist)
        return int(written)
    except Exception as e:
        LOG.exception("❌ [market_job] Error: %s", e)
        return 0

def main():
    logging.basicConfig(level=logging.INFO)
    run()

if __name__ == "__main__":
    main()
