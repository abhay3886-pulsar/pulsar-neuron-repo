from __future__ import annotations

from dataclasses import dataclass

from ...lib.exec import kite
from ...lib.guard.session_guard import SessionGuard, SessionStats
from ...lib.intent.schema import EntryIntent, EntrySpec, SLSpec, TPSpec, validate_intent
from ...lib.policy.index_v0 import initial_sl, scan_long_setup, scan_short_setup
from ...lib.state.fsm import AgentState
from ...lib.telemetry.logger import setup_logger


@dataclass
class BrainConfig:
    max_trades_per_day: int = 3
    vwap_band_pct: float = 0.6
    rr: float = 2.0


class IndexBrain:
    """Plain-Python FSM-style brain shell. Wire feeds & state machine in runner."""

    def __init__(self, config: BrainConfig | None = None) -> None:
        self.state = AgentState.IDLE
        self.config = config or BrainConfig()
        self.guard = SessionGuard(max_trades=self.config.max_trades_per_day)
        self.stats = SessionStats()
        self.log = setup_logger()

    def analyze(self, ctx) -> dict | None:
        long = scan_long_setup(ctx)
        short = scan_short_setup(ctx)
        return long or short

    def build_intent(self, setup: dict, ctx) -> EntryIntent:
        entry = float(setup["entry_level"])
        sl = float(initial_sl(entry, ctx, setup["side"]))
        if setup["side"] == "BUY":
            tp = entry + (entry - sl) * self.config.rr
        else:
            tp = entry - (sl - entry) * self.config.rr
        intent = EntryIntent(
            ts=str(ctx.ts),
            symbol=ctx.symbol,
            side=setup["side"],
            qty=1,
            entry=EntrySpec(type="LIMIT", price=entry),
            sl=SLSpec(type="SL-M", level=sl),
            tp=TPSpec(type="LIMIT", level=tp),
            rr=self.config.rr,
            reason=setup.get("reason", ""),
        )
        validate_intent(intent)
        return intent

    def act(self, intent: EntryIntent) -> dict:
        return kite.place(intent.model_dump())
