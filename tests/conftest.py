"""Pytest fixtures for Pulsar Neuron stubs."""

from __future__ import annotations

from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from pulsar_neuron.lib.schemas import Action, ContextHints, Decision


@pytest.fixture()
def dummy_hints() -> ContextHints:
    """Return a minimal context hint object."""

    return ContextHints(symbol="NIFTY", now_ist="2025-01-01T09:30:00+05:30")


@pytest.fixture()
def dummy_decision() -> Decision:
    """Return a wait decision for testing."""

    return Decision(
        action=Action.wait,
        confidence=0,
        chosen_strategy=None,
        bull_case=["stub"],
        bear_case=["stub"],
        reasons=["stub"],
    )
