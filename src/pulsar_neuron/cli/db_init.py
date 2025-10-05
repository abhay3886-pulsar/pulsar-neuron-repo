from __future__ import annotations

from pulsar_neuron.db_legacy.postgres import migrate


def main():
    migrate()
    print("✅ DB initialized: ohlcv, fut_oi, options_chain, market_breadth")


if __name__ == "__main__":
    main()
