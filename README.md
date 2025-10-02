# pulsar-neuron-repo

Deterministic scaffolding for the Pulsar Neuron intraday advisory stack.

## Project structure
- `src/pulsar_neuron/config` — configuration defaults.
- `src/pulsar_neuron/lib` — typed schemas and pure stubs for data, refs, price-action, derivatives, and risk.
- `src/pulsar_neuron/strategies` — strategy checklist skeletons.
- `src/pulsar_neuron/agentic` — planner prompt templates, tool runner, verifier rails, and decision formatter.
- `src/pulsar_neuron/cli` — CLI entry points for advisory and replay stubs.
- `tests` — unit-test skeletons covering schemas, strategies, and agentic rails.

## Development
1. Install dependencies with `poetry install`.
2. Run quality gates via `make fmt`, `make lint`, `make type`, and `make test`.
3. Try the stub CLI with `make advisory` or `make replay`.

### Pulsar Neuron — LLM Brain with Deterministic Facts
- **Math in code** (lib): VWAP, slopes, ORB, PA, OI/PCR, Greeks/IV, RR, refs.
- **Agent** thinks **BULL vs BEAR** using only tool outputs, then proposes **LONG/SHORT/WAIT**.
- **Verifier** enforces rails: time (≥14:45 no-new), RR≥1.2, max positions, wall distance ≥ 0.3× EM.
- **Outputs**:
  - One-liner: `NIFTY 11:12 | WAIT | Reason=TimeCutoff`
  - JSON Decision with `action, confidence, chosen_strategy, bull_case, bear_case, reasons, overrides`.
