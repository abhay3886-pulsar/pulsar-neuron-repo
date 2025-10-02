"""Agentic scaffolding skeleton tests."""

from __future__ import annotations

from pulsar_neuron.agentic import decision as decision_utils, planner_llm, verifier
from pulsar_neuron.lib.schemas import Action, ContextHints, Decision


def test_build_llm_prompt_contains_templates(dummy_hints: ContextHints) -> None:
    prompt = planner_llm.build_llm_prompt(dummy_hints)
    assert "BULL" in prompt
    assert "BEAR" in prompt
    for key in ["action", "confidence", "chosen_strategy", "bull_case", "bear_case", "reasons"]:
        assert key in prompt


def test_apply_verifier_forces_wait() -> None:
    initial = Decision(
        action=Action.long,
        confidence=70,
        chosen_strategy="stub",
        bull_case=["stub"],
        bear_case=["stub"],
        reasons=["stub"],
    )
    context = {
        "now": "15:00",
        "no_new_after": "14:45",
        "rr_min": 1.2,
        "rr_value": 1.0,
        "open_positions": 2,
        "max_positions": 2,
        "wall_min_distance": 0.3,
        "wall_distance": 0.1,
    }
    updated = verifier.apply_verifier(initial, context)
    assert updated.action == Action.wait
    assert updated.overrides


def test_format_one_liner(dummy_decision: Decision) -> None:
    line = decision_utils.format_one_liner("NIFTY", "2025-01-01T09:30:00+05:30", dummy_decision)
    assert isinstance(line, str) and line
