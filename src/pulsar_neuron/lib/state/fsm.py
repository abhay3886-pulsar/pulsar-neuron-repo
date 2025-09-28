from __future__ import annotations

from enum import Enum


class AgentState(str, Enum):
    IDLE = "IDLE"
    SCAN = "SCAN"
    PENDING = "PENDING"
    IN_POSITION = "IN_POSITION"
    MANAGE = "MANAGE"
    EXIT = "EXIT"
    COOLDOWN = "COOLDOWN"
    HALT = "HALT"
