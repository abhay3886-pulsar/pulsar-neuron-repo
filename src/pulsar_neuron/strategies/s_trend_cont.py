"""Trend continuation strategy checklist stub."""

from __future__ import annotations

from pulsar_neuron.lib.schemas import CheckItem, StrategyCheck, Verdict


def checklist(context: dict) -> StrategyCheck:
    """Return the trend continuation strategy checklist.

    NOTE: This is a stub; do not implement trading math here.
    """

    return StrategyCheck(
        strategy="s_trend_cont",
        preconditions=[
            CheckItem(item="stub_pre", ok=False, detail="NOT_IMPLEMENTED"),
        ],
        confirmers=[CheckItem(item="stub_conf", ok=False)],
        blockers=[CheckItem(item="stub_block", ok=False)],
        score=0,
        overall=Verdict.mixed,
    )
