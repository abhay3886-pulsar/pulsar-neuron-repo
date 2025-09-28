import pandas as pd

from pulsar_neuron.lib.features.ctx_index import build_ctx_index, get_orb
from pulsar_neuron.lib.features.indicators import vwap_from_bars, volume_ratio


def _mk(ts, o, h, l, c, v):
    return {"ts": ts, "open": o, "high": h, "low": l, "close": c, "volume": v}


def test_vwap_and_volratio():
    df = pd.DataFrame(
        [
            _mk("2025-09-29 09:15", 100, 110, 95, 105, 10),
            _mk("2025-09-29 09:20", 105, 108, 104, 107, 12),
            _mk("2025-09-29 09:25", 107, 113, 106, 112, 20),
        ]
    )
    df["ts"] = pd.to_datetime(df["ts"])
    vw = vwap_from_bars(df)
    assert vw > 0
    vr = volume_ratio(df["volume"], n=2)  # current vs median of previous 2
    assert vr > 0


def test_orb_ready_and_ctx():
    d1 = pd.DataFrame(
        [
            _mk("2025-09-26", 1, 2, 0.5, 1.5, 0),
            _mk("2025-09-29", 1, 2, 0.5, 1.6, 0),
        ]
    )
    m15 = pd.DataFrame(
        [
            _mk("2025-09-29 09:15", 100, 110, 95, 105, 10),
            _mk("2025-09-29 09:30", 106, 112, 104, 111, 15),
            _mk("2025-09-29 09:45", 110, 114, 108, 113, 12),
        ]
    )
    m5 = pd.DataFrame(
        [
            _mk("2025-09-29 09:15", 100, 110, 95, 105, 10),
            _mk("2025-09-29 09:20", 105, 108, 104, 107, 12),
            _mk("2025-09-29 09:25", 107, 113, 106, 112, 20),
            _mk("2025-09-29 09:30", 112, 115, 110, 114, 18),
        ]
    )
    for df in (d1, m15, m5):
        df["ts"] = pd.to_datetime(df["ts"])

    orb = get_orb(m5)
    assert orb.ready is True
    assert orb.high >= orb.low

    ctx = build_ctx_index("NIFTY", d1, m15, m5)
    assert ctx.symbol == "NIFTY"
    assert ctx.price == 114
    assert ctx.orb.ready is True
    assert ctx.trend_15m.label in ("up", "down", "neutral")
    assert 0.0 <= ctx.vol_ratio_5m
