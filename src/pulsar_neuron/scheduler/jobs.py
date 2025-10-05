# -*- coding: utf-8 -*-
# src/pulsar_neuron/scheduler/jobs.py
from __future__ import annotations

import argparse, logging, signal, threading, time
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from typing import Callable, Dict, List, Optional, Set
from zoneinfo import ZoneInfo

from pulsar_neuron.config.loader import load_defaults, load_markets
from pulsar_neuron.timeutils import now_ist as _now_ist

# ingestors (each has a run(...) function)
from pulsar_neuron.ingestors import (
    ohlcv_job,
    fut_oi_job,
    options_job,
    breadth_vix_job,
    context_pack_job,
)

IST = ZoneInfo("Asia/Kolkata")
LOG = logging.getLogger(__name__)
if not LOG.handlers:
    h = logging.StreamHandler()
    h.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] - %(message)s"))
    LOG.addHandler(h)
LOG.setLevel(logging.INFO)

JOB_TABLE = [
    {"name": "ohlcv_5m",      "cadence_s": 60,  "note": "publish +10s after bar close; only closed bars"},
    {"name": "fut_oi",        "cadence_s": 90,  "note": "capture 09:20 baseline once; else intraday"},
    {"name": "options_chain", "cadence_s": 150, "note": "ATM ±N coverage, IV/Greeks"},
    {"name": "breadth_vix",   "cadence_s": 300, "note": "VIX; breadth if supported"},
    {"name": "context_pack",  "cadence_s": 45,  "note": "packs only when inputs fresh"},
]

@dataclass
class SchedulerState:
    last_5m_published: Set[str] = field(default_factory=set)  # f"{symbol}|{ts_iso}"
    last_5m_date: Optional[date] = None
    baseline_done_for_day: Optional[date] = None
    stop_event: threading.Event = field(default_factory=threading.Event)
    cadence_overrides: Dict[str, int] = field(default_factory=dict)

STATE = SchedulerState()

# ---------- helpers ----------
def now_ist() -> datetime:
    return _now_ist().astimezone(IST)

def is_trading_window(ts: datetime) -> bool:
    start = ts.replace(hour=9, minute=15, second=0, microsecond=0)
    end   = ts.replace(hour=15, minute=30, second=0, microsecond=0)
    return start <= ts <= end

def five_min_boundary(ts: datetime) -> datetime:
    m = (ts.minute // 5) * 5
    return ts.replace(minute=m, second=0, microsecond=0)

def after_bar_close_buffer(ts: datetime, buffer_seconds: int = 10) -> bool:
    boundary = five_min_boundary(ts)
    return ts >= (boundary + timedelta(seconds=buffer_seconds))

def get_symbols_from_config() -> List[str]:
    mk = load_markets() or {}
    return list(mk.get("indices", []) or ["NIFTY 50", "NIFTY BANK"])

def _tz_to_ist(ts: datetime) -> datetime:
    return ts.replace(tzinfo=IST) if ts.tzinfo is None else ts.astimezone(IST)

# ---------- job wrappers that just call ingestors ----------
def job_ohlcv_5m(cfg: dict, ts: datetime) -> None:
    boundary = five_min_boundary(ts)
    if STATE.last_5m_date != boundary.date():
        STATE.last_5m_published.clear()
        STATE.last_5m_date = boundary.date()

    if not is_trading_window(ts) or not after_bar_close_buffer(ts, 10):
        return

    symbols = get_symbols_from_config()
    # run returns the list of bars actually persisted (or at least emitted)
    bars = ohlcv_job.run(cfg, symbols, boundary)  # ingestor handles provider+repo
    # De-dup marker
    for b in bars or []:
        key = f'{b["symbol"]}|{boundary.isoformat()}'
        STATE.last_5m_published.add(key)

def job_fut_oi(cfg: dict, ts: datetime) -> None:
    if not is_trading_window(ts):
        return
    symbols = get_symbols_from_config()
    target = ts.replace(hour=9, minute=20, second=0, microsecond=0)

    if STATE.baseline_done_for_day != ts.date() and ts >= target and (ts - target) < timedelta(minutes=3):
        fut_oi_job.run(cfg, symbols, ts, baseline=True)
        STATE.baseline_done_for_day = ts.date()
        return
    fut_oi_job.run(cfg, symbols, ts, baseline=False)

def job_options_chain(cfg: dict, ts: datetime) -> None:
    if not is_trading_window(ts):
        return
    symbols = get_symbols_from_config()
    options_job.run(cfg, symbols, ts)

def job_breadth_vix(cfg: dict, ts: datetime) -> None:
    if not is_trading_window(ts):
        return
    breadth_vix_job.run(cfg, ts)

def job_context_pack(cfg: dict, ts: datetime) -> None:
    if not is_trading_window(ts):
        return
    boundary = five_min_boundary(ts)
    symbols = get_symbols_from_config()
    have_all = all(f"{s}|{boundary.isoformat()}" in STATE.last_5m_published for s in symbols)
    context_pack_job.run(cfg, ts, ready=have_all)

# ---------- public API ----------
def run_all_once(now_ist_str: str) -> None:
    cfg = load_defaults() or {}
    try:
        ts = datetime.fromisoformat(now_ist_str)
        ts = _tz_to_ist(ts)
    except Exception:
        ts = now_ist()

    job_ohlcv_5m(cfg, ts)
    job_fut_oi(cfg, ts)
    job_options_chain(cfg, ts)
    job_breadth_vix(cfg, ts)
    job_context_pack(cfg, ts)

def start_scheduler() -> None:
    cfg = load_defaults() or {}

    def loop(job_name: str, fn: Callable[[dict, datetime], None], cadence_s: int):
        LOG.info("⏱️ start job=%s cadence=%ss", job_name, cadence_s)
        while not STATE.stop_event.is_set():
            start = time.time()
            ts = now_ist()
            try:
                fn(cfg, ts)
            except Exception as e:
                LOG.exception("job %s failed: %s", job_name, e)
            elapsed = time.time() - start
            target = STATE.cadence_overrides.get(job_name, cadence_s)
            sleep_s = max(0.5, target - elapsed)
            STATE.stop_event.wait(sleep_s)
        LOG.info("🛑 stopped job=%s", job_name)

    dispatch: Dict[str, Callable[[dict, datetime], None]] = {
        "ohlcv_5m": job_ohlcv_5m,
        "fut_oi": job_fut_oi,
        "options_chain": job_options_chain,
        "breadth_vix": job_breadth_vix,
        "context_pack": job_context_pack,
    }

    threads: List[threading.Thread] = []
    for row in JOB_TABLE:
        name, cadence = row["name"], int(row["cadence_s"])
        fn = dispatch[name]
        t = threading.Thread(target=loop, args=(name, fn, cadence), name=f"job/{name}", daemon=True)
        t.start()
        threads.append(t)

    LOG.info("✅ scheduler started with %d jobs", len(threads))
    try:
        while not STATE.stop_event.is_set():
            STATE.stop_event.wait(1.0)
    except KeyboardInterrupt:
        STATE.stop_event.set()
    finally:
        for t in threads:
            t.join(timeout=3.0)
        LOG.info("✅ scheduler stopped cleanly")

# ---------- CLI ----------
def _graceful_stop(_sig, _frm): STATE.stop_event.set()

if __name__ == "__main__":
    parser = argparse.ArgumentParser("pulsar-neuron scheduler")
    sub = parser.add_subparsers(dest="cmd", required=True)
    p_start = sub.add_parser("start", help="run all scheduler jobs continuously")
    p_start.add_argument("--context-pack-cadence", type=int, default=None)
    p_once = sub.add_parser("run-once", help="run all jobs once for a given IST time")
    p_once.add_argument("--now", required=True, help="ISO ts e.g. 2025-10-06T09:20:00+05:30")
    args = parser.parse_args()
    signal.signal(signal.SIGINT, _graceful_stop); signal.signal(signal.SIGTERM, _graceful_stop)
    if args.cmd == "start":
        if args.context_pack_cadence:
            STATE.cadence_overrides["context_pack"] = args.context_pack_cadence
        start_scheduler()
    elif args.cmd == "run-once":
        run_all_once(args.now)
