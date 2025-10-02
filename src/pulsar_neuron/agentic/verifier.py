"""Deterministic verifier rails for Pulsar Neuron decisions."""

from __future__ import annotations

from typing import List

from pulsar_neuron.lib.schemas import Action, Decision


def verify_time(now: str | None, no_new_after: str | None) -> List[str]:
    """Ensure trades are not taken after the configured cutoff."""

    if not now or not no_new_after:
        return []
    if now >= no_new_after:
        return [f"Time guard triggered: {now} >= {no_new_after}"]
    return []


def verify_rr(rr_min: float | None, rr_value: float | None) -> List[str]:
    """Ensure the reward-to-risk meets minimum requirements."""

    if rr_min is None or rr_value is None:
        return []
    if rr_value < rr_min:
        return [f"RR guard triggered: {rr_value} < {rr_min}"]
    return []


def verify_positions(open_positions: int | None, max_positions: int | None) -> List[str]:
    """Ensure position count remains within limits."""

    if open_positions is None or max_positions is None:
        return []
    if open_positions >= max_positions:
        return [f"Positions guard triggered: {open_positions}/{max_positions}"]
    return []


def verify_wall_distance(min_required: float | None, distance_em: float | None) -> List[str]:
    """Ensure the trade idea is sufficiently far from OI walls."""

    if min_required is None or distance_em is None:
        return []
    if distance_em < min_required:
        return [f"Wall guard triggered: {distance_em} < {min_required}"]
    return []


def apply_verifier(decision: Decision, context: dict) -> Decision:
    """Apply all rail checks and override the decision if necessary."""

    overrides: list[str] = []
    overrides.extend(verify_time(context.get("now"), context.get("no_new_after")))
    overrides.extend(verify_rr(context.get("rr_min"), context.get("rr_value")))
    overrides.extend(
        verify_positions(context.get("open_positions"), context.get("max_positions"))
    )
    overrides.extend(
        verify_wall_distance(context.get("wall_min_distance"), context.get("wall_distance"))
    )

    if not overrides:
        if decision.overrides:
            return decision.model_copy(update={"overrides": list(decision.overrides)})
        return decision

    updated_overrides = list(decision.overrides) + overrides
    if decision.action in {Action.long, Action.short}:
        return decision.model_copy(update={"action": Action.wait, "overrides": updated_overrides})
    return decision.model_copy(update={"overrides": updated_overrides})
