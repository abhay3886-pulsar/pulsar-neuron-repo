"""Advisory CLI entry point producing a stub decision."""

from __future__ import annotations

import json

import click

from pulsar_neuron.agentic import decision as decision_utils, planner_llm
from pulsar_neuron.lib.schemas import Action, ContextHints, Decision


@click.command()
@click.option("--symbol", type=click.Choice(["NIFTY", "BANKNIFTY"]), required=True)
@click.option("--now", "now_ts", type=str, required=True, help="ISO timestamp in IST")
def main(symbol: str, now_ts: str) -> None:
    """Generate a stubbed advisory decision."""

    hints = ContextHints(symbol=symbol, now_ist=now_ts)
    prompt_text = planner_llm.build_llm_prompt(hints)
    decision_stub = Decision(
        action=Action.wait,
        confidence=0,
        chosen_strategy=None,
        bull_case=["NOT_IMPLEMENTED"],
        bear_case=["NOT_IMPLEMENTED"],
        reasons=["LLM planner not integrated"],
    )
    line = decision_utils.format_one_liner(symbol, now_ts, decision_stub)
    click.echo(line)
    click.echo(json.dumps(decision_stub.model_dump(), indent=2, sort_keys=True))
    if not prompt_text:
        raise SystemExit("Prompt generation failed")


if __name__ == "__main__":  # pragma: no cover
    main()
