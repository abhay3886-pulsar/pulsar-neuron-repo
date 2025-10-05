"""Fetch mock Options Chain via ingest.options_job and write to DB."""
from __future__ import annotations

from datetime import date, datetime

from pulsar_neuron.db_legacy.options_repo import upsert_many
from pulsar_neuron.ingest import options_job


def _maybe_parse_ts(value):
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value)
        except ValueError:
            return value
    return value


def main() -> None:
    rows = options_job.run(["NIFTY", "BANKNIFTY"], mode="mock", strikes=3)
    for row in rows:
        expiry = row.get("expiry")
        if isinstance(expiry, str):
            row["expiry"] = date.fromisoformat(expiry)
        row["ts_ist"] = _maybe_parse_ts(row.get("ts_ist"))
    n = upsert_many(rows)
    print(f"✅ upserted {n} options_chain rows")


if __name__ == "__main__":
    main()
