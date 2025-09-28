from __future__ import annotations

from prometheus_client import Counter

trades_total = Counter("trades_total", "Total trades placed", ["symbol"])
