    """Scheduler jobs
    NOTE: Stub module. Add real logic later.
    """

# JOB TABLE (cadences in seconds)
JOB_TABLE = [
    {"name": "ohlcv_5m", "cadence_s": 60, "note": "publish +10s after bar close; only closed bars"},
    {"name": "fut_oi", "cadence_s": 90, "note": "capture 09:20 baseline once"},
    {"name": "options_chain", "cadence_s": 150, "note": "ATM ±N coverage, IV/Greeks"},
    {"name": "breadth_vix", "cadence_s": 300, "note": ""},
    {"name": "context_pack", "cadence_s": 45, "note": "IB 45s; mid 75s; late 60s; depends on freshness"},
]

def run_all_once(now_ist: str): ...
def start_scheduler(): ...

