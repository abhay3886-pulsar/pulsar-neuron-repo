# src/pulsar_neuron/ingest/options_job.py
from __future__ import annotations
import logging
from pulsar_neuron.providers.kite_provider import KiteMarketProvider
from pulsar_neuron.db import options_repo

LOG = logging.getLogger(__name__)

def run():
    LOG.info("🧮 [options_job] Starting options chain ingest")
    try:
        provider = KiteMarketProvider()
        symbols = ["NIFTY 50", "NIFTY BANK"]
        all_rows = []
        for s in symbols:
            rows = provider.fetch_option_chain(s)
            all_rows.extend(rows)
        written = options_repo.upsert_many(all_rows)
        LOG.info("✅ [options_job] Stored %d option rows", written)
        return written
    except Exception as e:
        LOG.exception("❌ [options_job] Error: %s", e)
        return 0

def main():
    logging.basicConfig(level=logging.INFO)
    run()

if __name__ == "__main__":
    main()
