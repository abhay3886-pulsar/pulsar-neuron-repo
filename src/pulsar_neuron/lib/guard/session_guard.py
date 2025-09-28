from __future__ import annotations

from dataclasses import dataclass


@dataclass
class SessionStats:
    trades_used: int = 0
    pnl_rupees: float = 0.0
    max_drawdown_pct: float = 0.0
    halted: bool = False


@dataclass
class SessionGuard:
    max_trades: int = 3
    max_dd_pct: float = 1.5
    cooldown_win_s: int = 300
    cooldown_loss_s: int = 900

    def can_enter(self, stats: SessionStats) -> bool:
        return (not stats.halted) and (stats.trades_used < self.max_trades)

    def halted_session(self, stats: SessionStats) -> bool:
        return stats.halted or (stats.max_drawdown_pct >= self.max_dd_pct)
