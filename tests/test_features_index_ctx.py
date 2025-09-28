import pandas as pd

from pulsar_neuron.lib.features.ctx_index import build_ctx_index


def _mk(ts, o, h, l, c, v):
    return {"ts": ts, "open": o, "high": h, "low": l, "close": c, "volume": v}


def test_build_ctx_minimal():
    dt = pd.to_datetime
    d1 = pd.DataFrame(
        [
            _mk(dt("2025-09-26"), 1, 2, 0.5, 1.5, 0),
            _mk(dt("2025-09-29"), 1, 2, 0.5, 1.6, 0),
        ]
    )
    m15 = pd.DataFrame(
        [
            _mk(dt("2025-09-29 09:15"), 100, 110, 95, 105, 10),
            _mk(dt("2025-09-29 09:30"), 106, 112, 104, 111, 15),
        ]
    )
    m5 = pd.DataFrame(
        [
            _mk(dt("2025-09-29 09:15"), 100, 110, 95, 105, 10),
            _mk(dt("2025-09-29 09:20"), 105, 108, 104, 107, 12),
            _mk(dt("2025-09-29 09:25"), 107, 113, 106, 112, 20),
        ]
    )
    for df in (d1, m15, m5):
        df["ts"] = pd.to_datetime(df["ts"])
    ctx = build_ctx_index("NIFTY", d1, m15, m5)
    assert ctx.symbol == "NIFTY"
    assert ctx.price == 112
    assert ctx.orb.high >= ctx.orb.low
    assert ctx.trend_15m.label in ("up", "down", "neutral")
