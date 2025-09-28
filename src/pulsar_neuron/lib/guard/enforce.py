from __future__ import annotations

import math
from typing import Tuple


def enforce_hard_rules(decision: dict, session_stats: dict) -> Tuple[bool, list[str]]:
    """
    Stateless checks on the brain_decide JSON.
    Returns (ok, errors[]). Do not mutate input.
    """

    errors: list[str] = []
    required = ["decision", "side", "entry", "sl", "target"]
    for k in required:
        if k not in decision:
            errors.append(f"missing:{k}")
    if errors:
        return False, errors
    if decision["decision"] not in ("take", "skip", "wait", "hold", "reduce", "exit"):
        errors.append("bad:decision")
    if decision["side"] not in ("long", "short", "null"):
        errors.append("bad:side")

    entry, sl, tp = float(decision["entry"]), float(decision["sl"]), float(decision["target"])
    if any(math.isnan(x) or math.isinf(x) for x in (entry, sl, tp)):
        errors.append("nan_or_inf")
    if decision["side"] == "long" and not (sl < entry < tp):
        errors.append("levels_order_long")
    if decision["side"] == "short" and not (tp < entry < sl):
        errors.append("levels_order_short")
    if session_stats.get("trades_used", 0) >= 3:
        errors.append("trades_cap_exceeded")
    return (len(errors) == 0), errors
