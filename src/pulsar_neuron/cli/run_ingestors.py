"""
CLI harness to test all mock ingestors end-to-end.
"""
from pulsar_neuron.ingest import ohlcv_job, fut_oi_job, options_job, market_job


def main():
    syms = ["NIFTY 50","NIFTY BANK"]
    print("▶️ OHLCV")
    bars = ohlcv_job.run(syms)
    print(f"✅ {sum(len(v) for v in bars.values())} bars")

    print("▶️ Futures OI")
    print(fut_oi_job.run(['NIFTY','BANKNIFTY']))

    print("▶️ Options Chain")
    chain = options_job.run(['NIFTY','BANKNIFTY'])
    print(f"✅ {len(chain)} option rows")

    print("▶️ Market Breadth / VIX")
    print(market_job.run())


if __name__ == "__main__":
    main()
