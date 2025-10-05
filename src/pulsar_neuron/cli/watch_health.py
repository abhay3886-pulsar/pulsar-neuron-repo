from __future__ import annotations
from pulsar_neuron.db.postgres import get_conn


def _one(sql: str, *args):
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, args)
        row = cur.fetchone()
        if not row:
            return None
        # support RealDictCursor or tuple
        return row.get("max") if isinstance(row, dict) else row[0]


def main():
    last_5m = _one("select max(ts_ist) as max from ohlcv where tf='5m'")
    last_15m = _one("select max(ts_ist) as max from ohlcv where tf='15m'")
    last_oi = _one("select max(ts_ist) as max from fut_oi")
    last_opt = _one("select max(ts_ist) as max from options_chain")
    last_breadth = _one("select max(ts_ist) as max from market_breadth")

    print("OHLCV 5m      :", last_5m)
    print("OHLCV 15m     :", last_15m)
    print("Futures OI    :", last_oi)
    print("Options Chain :", last_opt)
    print("Breadth/VIX   :", last_breadth)


if __name__ == "__main__":
    main()
