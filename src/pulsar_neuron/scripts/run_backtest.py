from __future__ import annotations

from ..lib.backtest.simulator import run_index_sim


if __name__ == "__main__":
    print(run_index_sim(["NIFTY", "BANKNIFTY"], "2025-08-01", "2025-08-31", {}))
