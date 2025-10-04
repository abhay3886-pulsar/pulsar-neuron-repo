"""Read latest OI and Options snapshot from DB and print small summaries."""
from __future__ import annotations

from pulsar_neuron.db.fut_oi_repo import read_last
from pulsar_neuron.db.options_repo import read_latest_snapshot


def main() -> None:
    for symbol in ["NIFTY", "BANKNIFTY"]:
        fut = read_last(symbol, limit=5)
        print(f"\n{symbol} — OI last {len(fut)}:")
        print(fut[:2])

        chain = read_latest_snapshot(symbol)
        strikes = sorted({row["strike"] for row in chain})
        print(
            f"{symbol} — latest snapshot strikes: {strikes[:6]}  (total rows: {len(chain)})"
        )


if __name__ == "__main__":
    main()
