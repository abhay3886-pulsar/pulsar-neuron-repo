"""LLM planning scaffolding for Pulsar Neuron."""

from __future__ import annotations

import json
from textwrap import dedent

from pulsar_neuron.lib.schemas import ContextHints

BULL_TEMPLATE = dedent(
    """
    BULL CASE TEMPLATE:
    - If bulls can defend key levels, describe the path for continuation.
    - Call out confirming evidence from VWAP, trend, and derivatives.
    - Highlight invalidation or conditions that would flip bias.
    """
).strip()

BEAR_TEMPLATE = dedent(
    """
    BEAR CASE TEMPLATE:
    - If bears seize control, describe the breakdown path.
    - Call out confirming evidence from VWAP, price action, and OI walls.
    - Highlight invalidation or conditions that would flip bias.
    """
).strip()

LLM_DECISION_SCHEMA = dedent(
    """
    Expected JSON keys:
    {
      "action": "long|short|wait",
      "confidence": 0-100,
      "chosen_strategy": string|null,
      "bull_case": [string, ...],
      "bear_case": [string, ...],
      "reasons": [string, ...]
    }
    Do not add extra keys.
    """
).strip()


def build_llm_prompt(hints: ContextHints) -> str:
    """Return the deterministic prompt for the reasoning LLM."""

    hints_blob = json.dumps(hints.model_dump(), indent=2, sort_keys=True)
    prompt = dedent(
        f"""
        Think like a professional intraday trader for NIFTY/BANKNIFTY.
        Always test BULL case and BEAR case.
        Use only supplied tool outputs; do not invent data.
        If mixed/weak → WAIT.
        Output JSON with {{action, confidence, chosen_strategy, bull_case, bear_case, reasons}} only.

        Context Hints:
        {hints_blob}

        {BULL_TEMPLATE}

        {BEAR_TEMPLATE}

        {LLM_DECISION_SCHEMA}
        """
    ).strip()
    return prompt
