# src/pulsar_neuron/ingest/fut_oi_job.py
from __future__ import annotations
import logging
from pulsar_neuron.providers.kite_provider import KiteMarketProvider
from pulsar_neuron.db import fut_oi_repo

LOG = logging.getLogger(__name__)

def run():
    LOG.info("📊 [fut_oi_job] Starting futures OI ingest")
    try:
        provider = KiteMarketProvider()
        symbols = ["NIFTY 50", "NIFTY BANK"]
        rows = provider.fetch_fut_oi(symbols)
        written = fut_oi_repo.upsert_many(rows)
        LOG.info("✅ [fut_oi_job] Stored %d fut_oi rows", written)
        return written
    except Exception as e:
        LOG.exception("❌ [fut_oi_job] Error: %s", e)
        return 0

def main():
    logging.basicConfig(level=logging.INFO)
    run()

if __name__ == "__main__":
    main()
