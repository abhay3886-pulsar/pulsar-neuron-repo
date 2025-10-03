# Architecture — Postgres-only (Agent Polling)
_Last updated: 2025-10-03 19:07 IST_

- EC2 runs: Scheduler, Ingest, Normalize, Context Builder, Agent, Verifier, Telemetry.
- RDS Postgres: hot store for OHLCV, OI, option_chain, baselines, context_pack, decisions.
- S3 (optional): Parquet cold history.
- Agent polls Postgres for latest context every 30–60s (IB), 60–90s (mid/late).
- Freshness SLAs (fail-closed) and rails (time/RR/wall/positions) enforced.
