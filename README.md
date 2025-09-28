# pulsar-neuron-repo

**pulsar_neuron** = deterministic trading toolkit (**lib**) + agent brains (**agentic**) for **NIFTY/BANKNIFTY**.  
Runtime: **Python 3.12** (Poetry-managed)

## Principles
- Deterministic core (no LLM required).
- Agent orchestrates lib (FSM now; optional LLM later).
- Safety-first: guardrails, idempotency, observability.
- Secrets-first: support **AWS Secrets Manager** + **SSM**, with local `.env` fallback.

## Layout
- `src/pulsar_neuron/lib` — data, features, guards, intents, exec, telemetry, storage, backtest
- `src/pulsar_neuron/agentic` — brains (index FSM, optional LLM)
- `src/pulsar_neuron/scripts` — entrypoints
- `infra/iam` — sample IAM policies for secrets
- `tests` — deterministic unit tests

## Quickstart
```bash
make setup
cp .env.example .env
make test
make run-index
```

## Secrets

Set `PULSAR_SECRETS_PROVIDER` to:

* `env` (default): read `.env`
* `aws`: load from **AWS Secrets Manager** (JSON secret), fallback to **SSM Parameter Store**
* `gcp`: load via **GCP Secret Manager** (stub provided)

See `src/pulsar_neuron/lib/config/secrets/aws.py` and `infra/iam/*` for details.

> CI uses stubs; no cloud calls are made in tests.
