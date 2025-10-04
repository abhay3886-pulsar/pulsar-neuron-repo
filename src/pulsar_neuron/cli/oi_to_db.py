"""Fetch mock Futures OI via ingest.fut_oi_job and write to DB."""
from __future__ import annotations

from datetime import datetime

from pulsar_neuron.db.fut_oi_repo import upsert_many
from pulsar_neuron.ingest import fut_oi_job


def _maybe_parse_ts(value):
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value)
        except ValueError:
            return value
    return value


def main() -> None:
    rows = fut_oi_job.run(["NIFTY", "BANKNIFTY"], mode="mock")
    for row in rows:
        row["ts_ist"] = _maybe_parse_ts(row.get("ts_ist"))
    n = upsert_many(rows)
    print(f"✅ upserted {n} fut_oi rows")


if __name__ == "__main__":
    main()
