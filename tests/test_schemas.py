"""Schema validations for Pulsar Neuron stubs."""

from __future__ import annotations

import json

import pytest

from pulsar_neuron.lib.schemas import (
    Action,
    Bias,
    CheckItem,
    ContextHints,
    Decision,
    EvidenceItem,
    StrategyCheck,
    Verdict,
)


def test_evidence_item_serializes() -> None:
    item = EvidenceItem(tool="dummy", ok=False, bias=Bias.na, value={"status": "NOT_IMPLEMENTED"})
    serialized = item.model_dump_json()
    payload = json.loads(serialized)
    assert payload["tool"] == "dummy"


def test_strategy_check_serializes() -> None:
    check = StrategyCheck(
        strategy="test",
        preconditions=[CheckItem(item="pre", ok=False)],
        confirmers=[],
        blockers=[],
        score=0,
        overall=Verdict.mixed,
    )
    assert "test" in check.model_dump_json()


def test_context_hints_and_decision(dummy_hints: ContextHints) -> None:
    decision = Decision(
        action=Action.wait,
        confidence=10,
        chosen_strategy="stub",
        bull_case=["stub"],
        bear_case=["stub"],
        reasons=["stub"],
    )
    assert dummy_hints.symbol == "NIFTY"
    assert json.loads(decision.model_dump_json())["action"] == "wait"


def test_enum_rejects_invalid() -> None:
    assert Bias.bull.value == "bull"
    with pytest.raises(ValueError):
        Bias("invalid")
    with pytest.raises(ValueError):
        Verdict("nope")
