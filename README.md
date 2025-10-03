# Pulsar Neuron (Postgres-only, Agent Polling)
Intraday agent for NIFTY & BANKNIFTY. **Math in code, reasoning in agent, safety in rails.**

## Pipeline
1. Ingest → 2. Normalize → 3. Context Pack → 4. Agent (LLM) → 5. Verifier (Rails) → 6. Output/Telemetry

## Principles
- Postgres-only hot path (no Redis). Agent **polls** latest context.
- Bars are **immutable after close** (+10s). 09:20 Futures OI baseline, 10:15 IB snapshot.
- Freshness SLAs (fail-closed): 5m≤90s, OI≤120s, Chain≤180s, Breadth/VIX≤300s.

## Docs
See `docs/` for architecture, swimlane, ERD, runbook, readiness.
