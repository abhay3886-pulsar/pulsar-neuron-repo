from __future__ import annotations

import datetime as dt
from dataclasses import dataclass

from ...lib.exec import kite
from ...lib.guard.session_guard import GuardConfig, SessionGuard, SessionStats, position_size_points
from ...lib.intent.schema import EntryIntent, EntrySpec, SLSpec, TPSpec
from ...lib.policy.index_v0 import initial_sl, scan_long_setup, scan_short_setup
from ...lib.state.fsm import AgentState
from ...lib.utils.common import lot_size, round_to_tick


@dataclass
class BrainConfig:
    max_trades_per_day: int = 3
    rr: float = 2.0
    risk_per_trade: float = 1000.0


class IndexBrain:
    """FSM-friendly brain converting context into intents."""

    def __init__(self, config: BrainConfig | None = None) -> None:
        self.state = AgentState.IDLE
        self.config = config or BrainConfig()
        self.guard = SessionGuard(
            GuardConfig(max_trades=self.config.max_trades_per_day, rr_min=self.config.rr)
        )
        self.stats = SessionStats()

    def can_enter(self, now: dt.datetime) -> bool:
        """External check for runners/graphs to gate new trades."""

        return self.guard.can_enter(now, self.stats)

    def analyze(self, ctx) -> dict | None:
        """Return first matching setup from deterministic policy scans."""

        long = scan_long_setup(ctx)
        if long:
            return long
        return scan_short_setup(ctx)

    def build_intent(self, setup: dict, ctx) -> EntryIntent:
        """Convert a policy setup into a validated entry intent."""

        side = setup["side"]
        entry = float(round_to_tick(setup["entry_level"]))
        sl = float(round_to_tick(initial_sl(entry, ctx, side)))
        if side == "BUY":
            target = float(round_to_tick(entry + (entry - sl) * self.config.rr))
        else:
            target = float(round_to_tick(entry - (sl - entry) * self.config.rr))
        if not self.guard.rr_ok(entry, sl, target, side):
            raise ValueError("Reward-risk below configured minimum.")
        sl_points = abs(entry - sl)
        qty = position_size_points(self.config.risk_per_trade, sl_points, lot_size(ctx.symbol))
        if qty <= 0:
            raise ValueError("Position size computed as zero. Check risk/sl inputs.")
        return EntryIntent(
            ts=str(ctx.ts),
            symbol=ctx.symbol,
            side=side,
            product="MIS",
            qty=qty,
            entry=EntrySpec(price=entry),
            sl=SLSpec(level=sl),
            tp=TPSpec(level=target),
            rr=self.config.rr,
            reason=setup.get("reason", ""),
        )

    def act(self, intent: EntryIntent) -> dict:
        """Submit the intent via execution shim."""

        return kite.place(intent.model_dump())
