# ─────────────────────────────────────────────
# PHONY targets (non-file)
# ─────────────────────────────────────────────
.PHONY: fmt lint type test migrate advisory run-spine run-ingestors token-check db-migrate db-oi-mock db-options-mock db-derivs-read run-live run-scheduler watch-health db-init db-smoke run-oi run-options run-market

# ─────────────────────────────────────────────
# Core maintenance
# ─────────────────────────────────────────────
fmt:
	@echo "🧹 format (stub)"

lint:
	@echo "🔍 lint (stub)"

type:
	@echo "🔠 type-check (stub)"

test:
	PYTHONPATH=src poetry run pytest -q

migrate:
	@echo "📦 apply sql in db/migrations (stub)"

advisory:
	@echo "📢 run advisory loop (stub)"

# ─────────────────────────────────────────────
# Main CLIs
# ─────────────────────────────────────────────
run-spine:
	poetry run python -m pulsar_neuron.cli.run_spine

run-ingestors:
	poetry run python -m pulsar_neuron.cli.run_ingestors

# ─────────────────────────────────────────────
# Database setup / mocks
# ─────────────────────────────────────────────
db-migrate:
	poetry run python -m pulsar_neuron.cli.db_init

db-oi-mock:
	poetry run python -m pulsar_neuron.cli.oi_to_db

db-options-mock:
	poetry run python -m pulsar_neuron.cli.options_to_db

db-derivs-read:
	poetry run python -m pulsar_neuron.cli.read_derivs

db-init:
	poetry run python -m pulsar_neuron.cli.db_init

db-smoke:
	poetry run python -m pulsar_neuron.cli.db_smoke

run-oi:
	poetry run python -m pulsar_neuron.ingest.fut_oi_job

run-options:
	poetry run python -m pulsar_neuron.ingest.options_job

run-market:
	poetry run python -m pulsar_neuron.ingest.market_job

# ─────────────────────────────────────────────
# Live & scheduler loops
# ─────────────────────────────────────────────
# Live bars (WebSocket → ohlcv 5m + derived 15m)
run-live:
	poetry run python -m pulsar_neuron.cli.live_bars

# Aligned scheduler (OI / Options / later breadth-vix)
run-scheduler:
	poetry run python -m pulsar_neuron.cli.run_scheduler

# Kite token visibility
token-check:
	poetry run python -m pulsar_neuron.cli.check_kite_token

# Health watcher (prints latest timestamps from DB)
watch-health:
	poetry run python -m pulsar_neuron.cli.watch_health

# ─────────────────────────────────────────────
# Usage example
# ─────────────────────────────────────────────
#   export AWS_REGION=ap-south-1
#   make db-init          # create schema
#   make db-smoke         # smoke test
#   make run-live         # process 1: emits 5m/15m
#   make run-scheduler    # process 2: OI/Options cadence
#   make watch-health     # sanity timestamps
