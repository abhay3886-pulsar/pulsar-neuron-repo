# src/pulsar_neuron/ingest/context_pack_job.py
from __future__ import annotations
import logging
from pulsar_neuron.service.context_pack import build_and_store_context_pack

LOG = logging.getLogger(__name__)

def run():
    LOG.info("🧠 [context_pack_job] Starting context pack build")
    try:
        count = build_and_store_context_pack()
        LOG.info("✅ [context_pack_job] Built %d context packs", count)
        return count
    except Exception as e:
        LOG.exception("❌ [context_pack_job] Error: %s", e)
        return 0

def main():
    logging.basicConfig(level=logging.INFO)
    run()

if __name__ == "__main__":
    main()
