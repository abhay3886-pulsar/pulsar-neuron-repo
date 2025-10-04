"""
Test harness for all ingestors (mock-only).
"""
from pulsar_neuron.ingest import fut_oi_job, market_job, ohlcv_job, options_job


def main():
    symbols = ["NIFTY 50", "NIFTY BANK"]
    print("▶️  OHLCV (mock)")
    bars = ohlcv_job.run(symbols, tf="5m", mode="mock")
    print(f"✅ {sum(len(v) for v in bars.values())} bars fetched")

    print("▶️  Futures OI (mock)")
    print(fut_oi_job.run(["NIFTY", "BANKNIFTY"], mode="mock"))

    print("▶️  Options Chain (mock)")
    opt = options_job.run(["NIFTY", "BANKNIFTY"], mode="mock")
    print(f"✅ {len(opt)} option records")

    print("▶️  Market Breadth / VIX (mock)")
    print(market_job.run())


if __name__ == "__main__":
    main()
