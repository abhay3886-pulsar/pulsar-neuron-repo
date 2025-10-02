"""Typed schemas and enums used across the Pulsar Neuron project."""

from __future__ import annotations

from enum import Enum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class Bias(str, Enum):
    """Directional leaning for a given evidence item."""

    bull = "bull"
    bear = "bear"
    neutral = "neutral"
    na = "na"


class Verdict(str, Enum):
    """Overall pass/fail status for a checklist."""

    pass_ = "pass"
    fail = "fail"
    mixed = "mixed"


class Action(str, Enum):
    """Trading action recommended by the agent."""

    long = "long"
    short = "short"
    wait = "wait"


class EvidenceItem(BaseModel):
    """Container for any tool output that feeds the decision process."""

    model_config = ConfigDict(extra="forbid")

    tool: str
    ok: bool
    bias: Bias
    value: dict[str, Any] = Field(default_factory=dict)
    explain: str | None = None


class CheckItem(BaseModel):
    """Granular pass/fail item inside a strategy checklist."""

    model_config = ConfigDict(extra="forbid")

    item: str
    ok: bool
    detail: str | None = None


class StrategyCheck(BaseModel):
    """Represents the evaluation of a trading strategy checklist."""

    model_config = ConfigDict(extra="forbid")

    strategy: str
    preconditions: list[CheckItem] = Field(default_factory=list)
    confirmers: list[CheckItem] = Field(default_factory=list)
    blockers: list[CheckItem] = Field(default_factory=list)
    score: int = 0
    overall: Verdict = Verdict.mixed


class ContextHints(BaseModel):
    """Minimal hints shared with the LLM planner to craft a decision."""

    model_config = ConfigDict(extra="forbid")

    now_ist: str
    symbol: str
    price_vs_vwap: str | None = None
    slope15: str | None = None
    orb: str | None = None
    nearest_oi_wall_distance_em: float | None = None
    expected_move_pts: float | None = None


class Decision(BaseModel):
    """Final recommended action produced by the planner and verifier."""

    model_config = ConfigDict(extra="forbid")

    action: Action
    confidence: int
    chosen_strategy: str | None
    bull_case: list[str] = Field(default_factory=list)
    bear_case: list[str] = Field(default_factory=list)
    reasons: list[str] = Field(default_factory=list)
    overrides: list[str] = Field(default_factory=list)
