from __future__ import annotations

import importlib
import logging
import os
import signal
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, time as dtime, timedelta
from typing import Callable, Dict, Optional
from zoneinfo import ZoneInfo

# --- IST everywhere ---
IST = ZoneInfo("Asia/Kolkata")


# --------------------------
# Logging (IST timestamps)
# --------------------------
def _setup_logging() -> None:
    log = logging.getLogger()
    if log.handlers:
        return  # already configured

    log.setLevel(logging.INFO)
    log_path = os.getenv("PULSAR_NEURON_LOG", "/tmp/pulsar_neuron.log")

    # Force asctime to IST
    class ISTFormatter(logging.Formatter):
        converter = staticmethod(lambda *args: datetime.now(IST).timetuple())

    fmt = ISTFormatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s", "%Y-%m-%d %H:%M:%S")

    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)
    fh = logging.FileHandler(log_path)
    fh.setFormatter(fmt)

    log.addHandler(sh)
    log.addHandler(fh)


logger = logging.getLogger(__name__)


# --------------------------
# Market hours (NSE)
# --------------------------
MARKET_OPEN = dtime(9, 15)    # 09:15 IST
MARKET_CLOSE = dtime(15, 30)  # 15:30 IST

def is_market_open(now: datetime) -> bool:
    """True if Mon–Fri and within trading window (inclusive)."""
    if now.tzinfo is None:
        now = now.replace(tzinfo=IST)
    now = now.astimezone(IST)
    if now.weekday() >= 5:  # 5=Sat, 6=Sun
        return False
    return (MARKET_OPEN <= now.time() <= MARKET_CLOSE)


# --------------------------
# Job definition
# --------------------------
@dataclass
class Job:
    name: str
    module: str                # python module path like "pulsar_neuron.ingest.ohlcv_job"
    func: str = "run"          # callable to invoke
    cadence_s: int = 60        # how often to run (seconds)
    post_close_offset_s: int = 0  # add delay after bar close (e.g., 10s)
    require_market_open: bool = True
    enabled: bool = True
    last_run: Optional[datetime] = None
    next_run: Optional[datetime] = None

    def schedule_next(self, now: datetime) -> None:
        base = now
        if self.last_run:
            base = max(self.last_run + timedelta(seconds=self.cadence_s), now)
        else:
            base = now

        # If job is aligned to bar close, add offset (e.g., run 10s after 5m boundary)
        if self.post_close_offset_s:
            # Round up to the next cadence boundary then add offset
            remainder = (base.minute * 60 + base.second) % self.cadence_s
            if remainder:
                base = base + timedelta(seconds=(self.cadence_s - remainder))
            base = base + timedelta(seconds=self.post_close_offset_s)
        self.next_run = base


# --------------------------
# Job table (edit to taste)
# --------------------------
# All times & cadence logic are in IST.
JOBS: Dict[str, Job] = {
    # 5m OHLCV: run every 60s tick but aligned to close +10s.
    "ohlcv_5m": Job(
        name="ohlcv_5m",
        module="pulsar_neuron.ingest.ohlcv_job",
        func="run",
        cadence_s=60,                 # tick every minute
        post_close_offset_s=10,       # +10s after the 5m bar close
        require_market_open=True,
        enabled=True,
    ),
    # Futures OI snapshots (baseline + periodic); here 90s cadence as a good default
    "fut_oi": Job(
        name="fut_oi",
        module="pulsar_neuron.ingest.fut_oi_job",
        func="run",
        cadence_s=90,
        post_close_offset_s=0,
        require_market_open=True,
        enabled=True,
    ),
    # Options chain with IV/Greeks (ATM ± window)
    "options_chain": Job(
        name="options_chain",
        module="pulsar_neuron.ingest.options_job",
        func="run",
        cadence_s=150,
        post_close_offset_s=0,
        require_market_open=True,
        enabled=True,
    ),
    # VIX / market breadth (if supported)
    "breadth_vix": Job(
        name="breadth_vix",
        module="pulsar_neuron.ingest.market_job",
        func="run",
        cadence_s=300,
        post_close_offset_s=0,
        require_market_open=True,
        enabled=True,
    ),
    # Context pack builder (joins latest slices)
    "context_pack": Job(
        name="context_pack",
        module="pulsar_neuron.ingest.context_pack_job",
        func="run",
        cadence_s=300,   # every 5 minutes
        post_close_offset_s=0,
        require_market_open=True,
        enabled=True,
    ),
}


# --------------------------
# Helpers
# --------------------------
def _call_job(job: Job) -> None:
    """Safely import and run a job's callable."""
    mod = importlib.import_module(job.module)
    fn = getattr(mod, job.func, None)
    if not callable(fn):
        raise RuntimeError(f"{job.module}.{job.func} is not callable")
    logger.info("▶️  Running job: %s (%s.%s)", job.name, job.module, job.func)
    # Allow both no-arg and (now_ist) signatures:
    try:
        fn()
    except TypeError:
        # try with current IST timestamp
        fn(datetime.now(IST))


def _due(job: Job, now: datetime) -> bool:
    return job.enabled and (job.next_run is None or now >= job.next_run)


# --------------------------
# Main loop
# --------------------------
_STOP = False

def _signal_handler(signum, frame):
    global _STOP
    _STOP = True
    logger.info("🛑 Received signal %s, stopping scheduler...", signum)

def start_scheduler(tick_seconds: int = 60) -> None:
    """
    Loop forever:
      - check market hours if required
      - run due jobs
      - reschedule each job after run
      - sleep tick_seconds between loops
    """
    _setup_logging()
    logger.info("🚀 Pulsar Neuron scheduler started (IST).")
    signal.signal(signal.SIGINT, _signal_handler)
    signal.signal(signal.SIGTERM, _signal_handler)

    # initialize job next_run times
    now = datetime.now(IST)
    for job in JOBS.values():
        job.schedule_next(now)

    while not _STOP:
        now = datetime.now(IST)

        for job in JOBS.values():
            if not _due(job, now):
                continue

            if job.require_market_open and not is_market_open(now):
                # skip but still nudge next_run forward to avoid tight loops pre-open
                job.schedule_next(now + timedelta(seconds=job.cadence_s))
                logger.debug("⏸️  Market closed; skipping job %s", job.name)
                continue

            try:
                _call_job(job)
                job.last_run = now
            except Exception as e:
                logger.exception("❌ Job %s failed: %s", job.name, e)
            finally:
                job.schedule_next(datetime.now(IST))

        time.sleep(tick_seconds)


# For `python -m pulsar_neuron.scheduler.jobs`
if __name__ == "__main__":
    start_scheduler()
