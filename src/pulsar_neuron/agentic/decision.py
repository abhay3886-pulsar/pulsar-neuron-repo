"""Utilities for presenting agent decisions."""

from __future__ import annotations

from pulsar_neuron.lib.schemas import Decision


def format_one_liner(symbol: str, now: str, decision: Decision) -> str:
    """Return a compact human-readable summary of a decision."""

    action_text = decision.action.value.upper()
    parts = [symbol, now, action_text, f"Conf={decision.confidence}%"]
    if decision.chosen_strategy:
        parts.append(f"Strat={decision.chosen_strategy}")
    if decision.reasons:
        parts.append(f"Reason={decision.reasons[0]}")
    if decision.overrides:
        parts.append(f"Overrides={decision.overrides[0]}")
    return " | ".join(parts)
