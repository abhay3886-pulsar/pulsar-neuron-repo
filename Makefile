.PHONY: fmt lint type test advisory replay
fmt:
	ruff --fix .
lint:
	ruff .
type:
	mypy src
test:
	pytest -q
advisory:
	PYTHONPATH=src poetry run python -m pulsar_neuron.cli.advisory --symbol NIFTY --now "2025-10-01T11:15:00+05:30"
replay:
	PYTHONPATH=src poetry run python -m pulsar_neuron.cli.replay --symbol NIFTY --date "2025-09-29"
