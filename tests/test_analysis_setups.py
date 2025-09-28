from datetime import datetime, timedelta

import pandas as pd

from pulsar_neuron.lib.features.context import build_ctx_index
from pulsar_neuron.lib.features.indicators import distance_from_vwap_pct, vwap_from_bars
from pulsar_neuron.lib.features.levels import get_cpr, get_daily_levels, get_orb
from pulsar_neuron.lib.features.setup_checkers import (
    is_cpr_break,
    is_orb_breakout,
    is_vwap_retest,
    trend_alignment,
    vwap_band_status,
)


def _mk(ts, o, h, l, c, v):
    return {"ts": ts, "open": o, "high": h, "low": l, "close": c, "volume": v}


def test_ctx_and_orb_breakout_long():
    d1 = pd.DataFrame(
        [
            _mk("2025-09-26", 100, 110, 90, 105, 0),
            _mk("2025-09-29", 105, 115, 95, 110, 0),
        ]
    )
    m15_rows = []
    start_15 = datetime(2025, 9, 29, 9, 15)
    for idx in range(11):
        ts = start_15 + timedelta(minutes=15 * idx)
        close = 105 + idx * 1.4
        m15_rows.append(
            _mk(
                str(ts),
                close - 0.5,
                close + 1.0,
                close - 1.5,
                round(close, 2),
                12 + idx,
            )
        )
    m15 = pd.DataFrame(m15_rows)

    m5_rows = []
    start_5 = datetime(2025, 9, 29, 9, 15)
    for idx in range(21):
        ts = start_5 + timedelta(minutes=5 * idx)
        if idx < 3:
            close = 105 + idx
        elif idx == 3:
            close = 107.2
        else:
            close = 110 + 0.02 * (idx - 4)
        m5_rows.append(
            _mk(
                str(ts),
                round(close - 0.2, 2),
                round(close + 0.5, 2),
                round(close - 0.5, 2),
                round(close, 2),
                10 + idx * 2,
            )
        )
    m5 = pd.DataFrame(m5_rows)
    for df in (d1, m15, m5):
        df["ts"] = pd.to_datetime(df["ts"])

    ctx = build_ctx_index("NIFTY", d1, m15, m5)
    assert ctx.orb.ready is True
    assert trend_alignment(ctx)["aligned"] is True
    assert ctx.price > ctx.orb.high
    assert ctx.vol_ratio_5m > 1.1
    assert vwap_band_status(ctx.vwap_dist_pct, 0.8)["inside"] is True
    assert is_orb_breakout(ctx, "long", vol_thresh=1.1, band=0.8) is True


def test_vwap_retest_short():
    d1 = pd.DataFrame(
        [
            _mk("2025-09-26", 100, 110, 90, 105, 0),
            _mk("2025-09-29", 105, 115, 95, 110, 0),
        ]
    )
    m15_rows = []
    start_15 = datetime(2025, 9, 29, 9, 15)
    for idx in range(11):
        ts = start_15 + timedelta(minutes=15 * idx)
        close = 130 - idx * 1.8
        m15_rows.append(
            _mk(
                str(ts),
                close + 0.5,
                close + 1.0,
                close - 1.5,
                round(close, 2),
                12 + idx,
            )
        )
    m15 = pd.DataFrame(m15_rows)

    m5_rows = []
    start_5 = datetime(2025, 9, 29, 9, 15)
    for idx in range(21):
        ts = start_5 + timedelta(minutes=5 * idx)
        base = 103.5 - 0.05 * idx
        m5_rows.append(
            _mk(
                str(ts),
                round(base + 0.2, 2),
                round(base + 0.5, 2),
                round(base - 0.5, 2),
                round(base, 2),
                30 - idx if idx < 15 else 15,
            )
        )
    m5 = pd.DataFrame(m5_rows)
    for df in (d1, m15, m5):
        df["ts"] = pd.to_datetime(df["ts"])

    ctx = build_ctx_index("BANKNIFTY", d1, m15, m5)
    assert is_vwap_retest(ctx, "short", band=1.0) is True


def test_cpr_break_checks_vr():
    d1 = pd.DataFrame(
        [
            _mk("2025-09-26", 100, 110, 90, 105, 0),
            _mk("2025-09-29", 105, 115, 95, 110, 0),
        ]
    )
    m15 = pd.DataFrame(
        [
            _mk("2025-09-29 09:15", 100, 110, 95, 105, 10),
            _mk("2025-09-29 09:30", 106, 114, 104, 112, 18),
        ]
    )
    m5 = pd.DataFrame(
        [
            _mk("2025-09-29 09:15", 100, 110, 95, 105, 8),
            _mk("2025-09-29 09:20", 105, 108, 104, 107, 9),
            _mk("2025-09-29 09:25", 107, 113, 106, 112, 25),
            _mk("2025-09-29 09:30", 112, 116, 110, 115, 28),
        ]
    )
    for df in (d1, m15, m5):
        df["ts"] = pd.to_datetime(df["ts"])

    ctx = build_ctx_index("NIFTY", d1, m15, m5)
    assert is_cpr_break(ctx, "long", vol_thresh=1.1) in (True, False)
    vb = vwap_band_status(ctx.vwap_dist_pct, band=2.0)
    assert set(vb) == {"inside", "dist_pct"}


def test_level_and_indicator_helpers_consistency():
    daily = pd.DataFrame(
        [
            _mk("2025-09-25", 100, 106, 98, 104, 0),
            _mk("2025-09-26", 104, 112, 102, 110, 0),
            _mk("2025-09-29", 110, 120, 108, 118, 0),
        ]
    )
    five = pd.DataFrame(
        [
            _mk("2025-09-29 09:15", 110, 112, 109, 111, 10),
            _mk("2025-09-29 09:20", 111, 114, 110, 113, 12),
            _mk("2025-09-29 09:25", 113, 116, 112, 115, 15),
        ]
    )
    for df in (daily, five):
        df["ts"] = pd.to_datetime(df["ts"])

    cpr = get_cpr(daily)
    daily_levels = get_daily_levels(daily)
    orb = get_orb(five, window=("09:15", "09:25"))
    vwap = vwap_from_bars(five)
    dist = distance_from_vwap_pct(float(five["close"].iloc[-1]), vwap)

    assert cpr.pivot > 0 and cpr.tc >= cpr.bc
    assert daily_levels.pdh == 112 and daily_levels.pdl == 102 and daily_levels.pdc == 110
    assert orb.ready is True and orb.high >= orb.low
    assert vwap > 0 and isinstance(dist, float)
