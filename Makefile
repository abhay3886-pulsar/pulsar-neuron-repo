.PHONY: setup lint format test run-index run-backtest precommit pyver
setup:
poetry install --with cloud
poetry run pre-commit install
lint:
poetry run black --check .
poetry run isort --check-only .
poetry run flake8
poetry run mypy src/pulsar_neuron
format:
poetry run isort .
poetry run black .
test:
poetry run pytest -q
run-index:
poetry run python -m pulsar_neuron.scripts.run_index_agent
run-backtest:
poetry run python -m pulsar_neuron.scripts.run_backtest
precommit:
pre-commit run --all-files
pyver:
python -c "import sys; print(sys.version)"
