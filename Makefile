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
