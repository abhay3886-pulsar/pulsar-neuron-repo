from __future__ import annotations

import datetime as dt
from dataclasses import dataclass


@dataclass
class SessionStats:
    trades_used: int = 0
    pnl_rupees: float = 0.0
    max_drawdown_pct: float = 0.0
    halted: bool = False
    cooldown_until: dt.datetime | None = None


@dataclass
class GuardConfig:
    max_trades: int = 3
    max_dd_pct: float = 1.5
    last_entry_cutoff_hm: tuple[int, int] = (14, 45)
    rr_min: float = 1.5


class SessionGuard:
    def __init__(self, cfg: GuardConfig | None = None) -> None:
        self.cfg = cfg or GuardConfig()

    def can_enter(self, now: dt.datetime, stats: SessionStats) -> bool:
        if stats.halted:
            return False
        if stats.trades_used >= self.cfg.max_trades:
            return False
        if stats.max_drawdown_pct >= self.cfg.max_dd_pct:
            return False
        if stats.cooldown_until and now < stats.cooldown_until:
            return False
        cutoff = now.replace(
            hour=self.cfg.last_entry_cutoff_hm[0],
            minute=self.cfg.last_entry_cutoff_hm[1],
            second=0,
            microsecond=0,
        )
        if now > cutoff:
            return False
        return True

    def rr_ok(self, entry: float, sl: float, tp: float, side: str) -> bool:
        risk = (entry - sl) if side == "BUY" else (sl - entry)
        reward = (tp - entry) if side == "BUY" else (entry - tp)
        if risk <= 0:
            return False
        return (reward / risk) >= self.cfg.rr_min


def position_size_points(risk_rupees: float, sl_points: float, lot_size: int) -> int:
    """Return quantity (in units, not lots) respecting rupee risk and points SL."""

    if sl_points <= 0 or lot_size <= 0:
        return 0
    qty = int(risk_rupees // (sl_points * 1.0))
    return (qty // lot_size) * lot_size
