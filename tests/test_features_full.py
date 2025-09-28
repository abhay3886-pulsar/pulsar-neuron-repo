import pandas as pd

from pulsar_neuron.lib.features.context import build_ctx_index
from pulsar_neuron.lib.features.indicators import distance_from_vwap_pct, vwap_from_bars
from pulsar_neuron.lib.features.levels import get_cpr, get_daily_levels, get_orb


def _mk(ts, o, h, l, c, v):
    return {"ts": ts, "open": o, "high": h, "low": l, "close": c, "volume": v}


def test_cpr_and_daily_levels():
    d1 = pd.DataFrame([
        _mk("2025-09-26", 1, 2, 0.5, 1.5, 0),
        _mk("2025-09-29", 1, 2, 0.5, 1.6, 0),
    ])
    d1["ts"] = pd.to_datetime(d1["ts"])
    cpr = get_cpr(d1)
    daily = get_daily_levels(d1)
    assert cpr.pivot > 0 and daily.pdh > 0


def test_vwap_distance_and_orb():
    m5 = pd.DataFrame(
        [
            _mk("2025-09-29 09:15", 100, 110, 95, 105, 10),
            _mk("2025-09-29 09:20", 105, 108, 104, 107, 12),
            _mk("2025-09-29 09:25", 107, 113, 106, 112, 20),
            _mk("2025-09-29 09:30", 112, 115, 110, 114, 18),
        ]
    )
    m5["ts"] = pd.to_datetime(m5["ts"])
    vw = vwap_from_bars(m5)
    dist = distance_from_vwap_pct(114, vw)
    orb = get_orb(m5)
    assert vw > 0 and abs(dist) < 5 and orb.ready


def test_build_ctx_index_full():
    d1 = pd.DataFrame([
        _mk("2025-09-26", 100, 110, 90, 105, 1000),
        _mk("2025-09-29", 105, 115, 95, 110, 1200),
    ])
    d1["ts"] = pd.to_datetime(d1["ts"])
    m15 = pd.DataFrame([
        _mk("2025-09-29 09:15", 100, 110, 95, 105, 10),
        _mk("2025-09-29 09:30", 106, 112, 104, 111, 15),
    ])
    m15["ts"] = pd.to_datetime(m15["ts"])
    m5 = pd.DataFrame([
        _mk("2025-09-29 09:15", 100, 110, 95, 105, 10),
        _mk("2025-09-29 09:20", 105, 108, 104, 107, 12),
    ])
    m5["ts"] = pd.to_datetime(m5["ts"])
    ctx = build_ctx_index("NIFTY", d1, m15, m5)
    assert ctx.price > 0
    assert ctx.cpr.pivot > 0
    assert ctx.daily.pdh > 0
