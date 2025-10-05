# src/pulsar_neuron/scheduler/jobs.py

"""Scheduler jobs for Pulsar Neuron.

- Cadences (sec) are hints; each job enforces its own guards.
- 5m OHLCV publishes only *closed* bars with a +10s buffer after bar close.
- Futures OI baseline captured exactly once at 09:20 IST each trading day.
- Options chain fetch covers ATM ±N (implemented in provider) and computes IV/Greeks.
- Breadth + VIX every 5m (if supported by provider).
- Context pack runner is a placeholder that depends on data freshness.

Wire your DB upsert or message-publishers in the `on_*` hooks below.

Environment/config:
- MARKET_PROVIDER: "kite" (default) or "mock"
- Config loader: load_defaults(), load_markets() (already in your repo)

"""

from __future__ import annotations

import logging
import os
import threading
import time
from dataclasses import dataclass, field
from datetime import date, datetime, time as dtime, timedelta
from typing import Any, Callable, Dict, Iterable, List, Optional, Set, Tuple
from zoneinfo import ZoneInfo

from pulsar_neuron.config.loader import load_defaults, load_markets
from pulsar_neuron.timeutils import now_ist as _now_ist

# Provider interface + factory (you added this in earlier steps)
from pulsar_neuron.providers.market_provider import MarketProvider

try:
    from pulsar_neuron.providers import get_provider  # registry you likely created
except Exception:
    get_provider = None  # fallback handled below

IST = ZoneInfo("Asia/Kolkata")
LOG = logging.getLogger(__name__)
LOG.setLevel(logging.INFO)

# ----------------------------- job table --------------------------------------

JOB_TABLE = [
    {"name": "ohlcv_5m", "cadence_s": 60, "note": "publish +10s after bar close; only closed bars"},
    {"name": "fut_oi", "cadence_s": 90, "note": "capture 09:20 baseline once; else live OI"},
    {"name": "options_chain", "cadence_s": 150, "note": "ATM ±N coverage, IV/Greeks"},
    {"name": "breadth_vix", "cadence_s": 300, "note": "best-effort if supported"},
    {"name": "context_pack", "cadence_s": 45, "note": "IB 45s; mid 75s; late 60s; depends on freshness"},
]


# ----------------------------- state ------------------------------------------

@dataclass
class SchedulerState:
    last_5m_published: Set[str] = field(default_factory=set)  # keys: f"{symbol}|{ts_iso}"
    baseline_done_for_day: Optional[date] = None
    stop_event: threading.Event = field(default_factory=threading.Event)
    cadence_overrides: Dict[str, int] = field(default_factory=dict)  # name->seconds


STATE = SchedulerState()


# ----------------------------- helpers ----------------------------------------

def now_ist() -> datetime:
    return _now_ist().astimezone(IST)


def is_trading_window(ts: datetime) -> bool:
    """Cash session 09:15–15:30 IST; allow a little pre/post slack for prep."""
    start = ts.replace(hour=9, minute=15, second=0, microsecond=0)
    end = ts.replace(hour=15, minute=30, second=0, microsecond=0)
    return start <= ts <= end


def five_min_boundary(ts: datetime) -> datetime:
    """Floor to the last 5-min boundary in IST."""
    m = (ts.minute // 5) * 5
    return ts.replace(minute=m, second=0, microsecond=0)


def after_bar_close_buffer(ts: datetime, buffer_seconds: int = 10) -> bool:
    """Return True if now is >= (last 5m boundary + buffer)."""
    boundary = five_min_boundary(ts)
    return ts >= (boundary + timedelta(seconds=buffer_seconds))


def get_symbols_from_config(cfg: dict) -> List[str]:
    markets = load_markets() or {}
    # prefer configured indices list; fallback to known common indices
    symbols = markets.get("indices", []) or ["NIFTY 50", "NIFTY BANK"]
    return list(symbols)


def make_provider(cfg: dict, logger: logging.Logger) -> MarketProvider:
    kind = (cfg.get("market", {}).get("provider") or os.getenv("MARKET_PROVIDER") or "kite").lower()
    if get_provider:
        return get_provider(kind, cfg, logger=logger)
    # Minimal fallback to avoid import errors in edge setups
    if kind == "mock":
        from pulsar_neuron.providers.mock_provider import MockMarketProvider
        return MockMarketProvider(cfg.get("market", {}).get("tz", "Asia/Kolkata"))
    from pulsar_neuron.providers.kite_provider import KiteMarketProvider
    return KiteMarketProvider(cfg, logger=logger)


# ----------------------------- output hooks -----------------------------------

def on_ohlcv_bars(bars: List[dict]) -> None:
    """TODO: replace with DB upsert or event publish."""
    if not bars:
        return
    LOG.info("🧾 OHLCV(5m) | rows=%d | sample=%s", len(bars), bars[-1])


def on_fut_oi(rows: List[dict]) -> None:
    """TODO: replace with DB upsert or event publish."""
    if not rows:
        return
    LOG.info("📊 Futures OI | rows=%d | sample=%s", len(rows), rows[-1])


def on_option_chain(rows: List[dict]) -> None:
    """TODO: replace with DB upsert or event publish."""
    if not rows:
        return
    LOG.info("🧮 Options | rows=%d | sample=%s", len(rows), rows[-1])


def on_breadth(b: dict | None) -> None:
    if b:
        LOG.info("🌿 Breadth | adv=%s dec=%s unch=%s @%s", b.get("adv"), b.get("dec"), b.get("unchanged"),
                 b.get("ts_ist"))


def on_vix(v: dict | None) -> None:
    if v:
        LOG.info("🦔 VIX | value=%.2f @%s", float(v.get("value", 0.0)), v.get("ts_ist"))


def on_context_packed(ok: bool) -> None:
    LOG.info("🧱 Context pack | status=%s", "ok" if ok else "skipped")


# ----------------------------- job impls --------------------------------------

def job_ohlcv_5m(prov: MarketProvider, cfg: dict, now: datetime) -> None:
    if not is_trading_window(now) or not after_bar_close_buffer(now, buffer_seconds=10):
        return
    symbols = get_symbols_from_config(cfg)
    boundary = five_min_boundary(now)
    since = boundary - timedelta(minutes=30)  # fetch a small window; we'll filter exact bar

    bars = prov.fetch_ohlcv(symbols, "5m", since=since)
    if not bars:
        return

    # de-dup per symbol|boundary
    filtered: List[dict] = []
    for b in bars:
        if b["ts_ist"].replace(tzinfo=IST) != boundary:
            continue
        key = f'{b["symbol"]}|{boundary.isoformat()}'
        if key in STATE.last_5m_published:
            continue
        STATE.last_5m_published.add(key)
        filtered.append(b)

    if filtered:
        filtered.sort(key=lambda r: (r["symbol"], r["ts_ist"]))
        on_ohlcv_bars(filtered)


def job_fut_oi(prov: MarketProvider, cfg: dict, now: datetime) -> None:
    if not is_trading_window(now):
        return
    symbols = get_symbols_from_config(cfg)

    # baseline once at 09:20 IST each trading day
    target = now.replace(hour=9, minute=20, second=0, microsecond=0)
    is_today = STATE.baseline_done_for_day == now.date()

    if not is_today and now >= target and (now - target) < timedelta(minutes=3):
        rows = prov.fetch_fut_oi(symbols)
        for r in rows:
            r["baseline_tag"] = "open_baseline"
        STATE.baseline_done_for_day = now.date()
        on_fut_oi(rows)
        return

    # otherwise live OI
    rows = prov.fetch_fut_oi(symbols)
    for r in rows:
        r["baseline_tag"] = "intraday"
    on_fut_oi(rows)


def job_options_chain(prov: MarketProvider, cfg: dict, now: datetime) -> None:
    if not is_trading_window(now):
        return
    symbols = get_symbols_from_config(cfg)
    # Quote each index individually to control coverage size and pacing within provider
    out: List[dict] = []
    for sym in symbols:
        rows = prov.fetch_option_chain(sym)
        out.extend(rows)
    if out:
        out.sort(key=lambda r: (r["symbol"], r["expiry"], r["strike"], r["side"], r["ts_ist"]))
        on_option_chain(out)


def job_breadth_vix(prov: MarketProvider, cfg: dict, now: datetime) -> None:
    # run within session; outside session it’s not that useful
    if not is_trading_window(now):
        return
    # breadth (if provider supports)
    try:
        b = prov.fetch_breadth()
    except NotImplementedError:
        b = None
    except Exception as e:
        LOG.warning("breadth failed: %s", e)
        b = None
    on_breadth(b)

    # vix
    try:
        v = prov.fetch_vix()
    except Exception as e:
        LOG.warning("vix failed: %s", e)
        v = None
    on_vix(v)


def job_context_pack(prov: MarketProvider, cfg: dict, now: datetime) -> None:
    """Placeholder: compute/pack compact context once inputs are fresh.
    You likely want to verify the last published 5m boundary is current before packing.
    """
    if not is_trading_window(now):
        return
    ok = True  # set False if any freshness check fails
    on_context_packed(ok)


# ----------------------------- public API -------------------------------------

def run_all_once(now_ist: str) -> None:
    """Run each job once with guards, interpreting `now_ist` as ISO in IST."""
    cfg = load_defaults() or {}
    prov = make_provider(cfg, LOG)
    try:
        ts = datetime.fromisoformat(now_ist)
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=IST)
        else:
            ts = ts.astimezone(IST)
    except Exception:
        ts = now_ist()  # fallback to real now if parsing fails

    job_ohlcv_5m(prov, cfg, ts)
    job_fut_oi(prov, cfg, ts)
    job_options_chain(prov, cfg, ts)
    job_breadth_vix(prov, cfg, ts)
    job_context_pack(prov, cfg, ts)


def start_scheduler() -> None:
    """Cooperative in-process scheduler using per-job loops.

    - Honors cadence from JOB_TABLE (can be overridden via STATE.cadence_overrides).
    - You can stop it by setting STATE.stop_event.
    """
    cfg = load_defaults() or {}
    prov = make_provider(cfg, LOG)

    def loop(job_name: str, fn: Callable[[MarketProvider, dict, datetime], None], cadence_s: int):
        LOG.info("⏱️  start job=%s cadence=%ss", job_name, cadence_s)
        while not STATE.stop_event.is_set():
            start = time.time()
            ts = now_ist()
            try:
                fn(prov, cfg, ts)
            except Exception as e:
                LOG.exception("Job %s failed: %s", job_name, e)
            # sleep the remaining time in cadence, but never negative
            elapsed = time.time() - start
            target = STATE.cadence_overrides.get(job_name, cadence_s)
            time_to_sleep = max(0.5, target - elapsed)
            STATE.stop_event.wait(time_to_sleep)
        LOG.info("🛑 stopped job=%s", job_name)

    # map names to functions
    dispatch: Dict[str, Callable[[MarketProvider, dict, datetime], None]] = {
        "ohlcv_5m": job_ohlcv_5m,
        "fut_oi": job_fut_oi,
        "options_chain": job_options_chain,
        "breadth_vix": job_breadth_vix,
        "context_pack": job_context_pack,
    }

    threads: List[threading.Thread] = []
    for row in JOB_TABLE:
        name = row["name"]
        cadence = int(row["cadence_s"])
        fn = dispatch.get(name)
        if not fn:
            LOG.warning("unknown job in table: %s", name)
            continue
        t = threading.Thread(target=loop, args=(name, fn, cadence), name=f"job/{name}", daemon=True)
        t.start()
        threads.append(t)

    LOG.info("✅ Scheduler started with %d jobs", len(threads))
    # Keep main thread alive; return when stop_event is set.
    try:
        while not STATE.stop_event.is_set():
            STATE.stop_event.wait(1.0)
    except KeyboardInterrupt:
        LOG.info("KeyboardInterrupt: stopping scheduler…")
        STATE.stop_event.set()
    finally:
        for t in threads:
            t.join(timeout=3.0)
        LOG.info("✅ Scheduler stopped cleanly")
