from __future__ import annotations

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from pulsar_neuron import db as db_module
from pulsar_neuron.ingest import fut_oi_job
from pulsar_neuron.normalize import normalize_option_chain, normalize_ohlcv


IST = ZoneInfo("Asia/Kolkata")


def test_normalize_ohlcv_skips_partial(monkeypatch):
    fixed_now = datetime(2024, 8, 1, 10, 20, tzinfo=IST)
    monkeypatch.setattr("pulsar_neuron.timeutils.now_ist", lambda: fixed_now)

    bars = [
        {
            "symbol": "NIFTY 50",
            "tf": "5m",
            "ts_ist": fixed_now - timedelta(minutes=10),
            "o": 100.0,
            "h": 101.0,
            "l": 99.0,
            "c": 100.5,
            "v": 1_000,
        },
        {
            "symbol": "NIFTY 50",
            "tf": "5m",
            "ts_ist": fixed_now - timedelta(minutes=5),
            "o": 101.0,
            "h": 102.0,
            "l": 100.0,
            "c": 101.5,
            "v": 1_200,
        },
        {
            "symbol": "NIFTY 50",
            "tf": "5m",
            "ts_ist": fixed_now - timedelta(minutes=2),
            "o": 102.0,
            "h": 103.0,
            "l": 101.0,
            "c": 102.5,
            "v": 1_100,
        },
    ]

    normalized = normalize_ohlcv(bars)
    assert len(normalized) == 2

    captured: list[list] = []

    def fake_upsert(rows):
        captured.append(rows)

    monkeypatch.setattr(db_module, "upsert_ohlcv", fake_upsert)
    db_module.upsert_ohlcv(normalized)
    assert captured and len(captured[0]) == 2


def test_option_chain_clipping(caplog):
    caplog.set_level("WARNING")
    fixed_now = datetime(2024, 8, 1, 10, 0, tzinfo=IST)
    rows = [
        {
            "symbol": "NIFTY 50",
            "ts_ist": fixed_now,
            "expiry": "2024-08-29",
            "strike": 20000,
            "side": "CE",
            "ltp": 150.0,
            "iv": 420.0,
            "oi": 10_000,
            "doi": 500,
            "volume": 1_000,
            "delta": 1.4,
            "gamma": float("inf"),
            "theta": -2_000.0,
            "vega": float("nan"),
        }
    ]
    normalized = normalize_option_chain(rows)
    assert normalized[0]["iv"] == 200.0
    assert normalized[0]["delta"] == 1.0
    assert normalized[0]["gamma"] is None
    assert normalized[0]["theta"] == -1000.0
    assert normalized[0]["vega"] is None
    assert any("Clipped" in record.message for record in caplog.records)


def test_fut_oi_baseline_once(monkeypatch):
    fixed_now = datetime(2024, 8, 1, 9, 22, tzinfo=IST)
    monkeypatch.setattr(fut_oi_job, "now_ist", lambda: fixed_now)

    class DummyProvider:
        def fetch_fut_oi(self, symbols):
            return [
                {
                    "symbol": symbols[0],
                    "ts_ist": fixed_now,
                    "price": 100.0,
                    "oi": 1000,
                }
            ]

    monkeypatch.setattr(fut_oi_job, "resolve_provider", lambda logger=None: DummyProvider())
    monkeypatch.setattr(fut_oi_job, "upsert_fut_oi", lambda rows: None)
    calls: list = []

    def record_baseline(rows, trading_day, tag):
        calls.append((trading_day, tag, rows))

    monkeypatch.setattr(fut_oi_job, "insert_or_update_fut_oi_baseline", record_baseline)
    fut_oi_job._baseline_written.clear()
    fut_oi_job.run(["NIFTY 50"])
    fut_oi_job.run(["NIFTY 50"])
    assert len(calls) == 1
    trading_day, tag, rows = calls[0]
    assert tag == "open_baseline"
    assert rows[0]["baseline_tag"] == "open_baseline"
