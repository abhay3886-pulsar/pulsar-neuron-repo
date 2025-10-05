# Placeholder targets
.PHONY: fmt lint type test migrate advisory run-spine

fmt:
	@echo "format (stub)"
lint:
	@echo "lint (stub)"
type:
	@echo "type-check (stub)"
test:
	@echo "pytest (stub)"
migrate:
	@echo "apply sql in db/migrations (stub)"
advisory:
	@echo "run advisory loop (stub)"

run-spine:
	poetry run python -m pulsar_neuron.cli.run_spine

run-ingestors:
	poetry run python -m pulsar_neuron.cli.run_ingestors

db-migrate:
	poetry run python -m pulsar_neuron.cli.db_init

db-oi-mock:
	poetry run python -m pulsar_neuron.cli.oi_to_db

db-options-mock:
	poetry run python -m pulsar_neuron.cli.options_to_db

db-derivs-read:
        poetry run python -m pulsar_neuron.cli.read_derivs

# Live bars (WebSocket → ohlcv 5m + derived 15m)
run-live:
        poetry run python -m pulsar_neuron.cli.live_bars

# Aligned scheduler (OI / Options / later breadth-vix)
run-scheduler:
        poetry run python -m pulsar_neuron.cli.run_scheduler

# Health watcher (prints latest timestamps from DB)
watch-health:
        poetry run python -m pulsar_neuron.cli.watch_health

db-init:
        poetry run python -m pulsar_neuron.cli.db_init

db-smoke:
        poetry run python -m pulsar_neuron.cli.db_smoke

# Usage:
#   export PGHOST=... PGDATABASE=... PGUSER=... PGPASSWORD=...
#   export KITE_API_KEY=... KITE_ACCESS_TOKEN=...
#   export KITE_TOKENS_JSON='{"NIFTY 50":256265,"NIFTY BANK":260105}'
#   make run-live          # process 1: emits 5m/15m
#   make run-scheduler     # process 2: OI/Options cadence
#   make watch-health      # sanity timestamps
