from __future__ import annotations

from typing import Dict, Iterable, List, Mapping

from .postgres import get_conn

UPSERT_SQL = """
insert into options_chain(symbol, ts_ist, expiry, strike, side, ltp, iv, oi, volume, delta, gamma, theta, vega)
values (%(symbol)s, %(ts_ist)s, %(expiry)s, %(strike)s, %(side)s, %(ltp)s, %(iv)s, %(oi)s, %(volume)s, %(delta)s, %(gamma)s, %(theta)s, %(vega)s)
on conflict (symbol, ts_ist, expiry, strike, side) do update set
  ltp = excluded.ltp,
  iv = excluded.iv,
  oi = excluded.oi,
  volume = excluded.volume,
  delta = excluded.delta,
  gamma = excluded.gamma,
  theta = excluded.theta,
  vega = excluded.vega;
"""

READ_LAST_TS_SQL = """
select ts_ist from options_chain
where symbol = %s
order by ts_ist desc
limit 1
"""

READ_SNAPSHOT_SQL = """
select symbol, ts_ist, expiry, strike, side, ltp, iv, oi, volume, delta, gamma, theta, vega
from options_chain
where symbol = %s and ts_ist = %s
order by strike asc, side asc
"""


def upsert_many(rows: Iterable[Mapping]) -> int:
    rows = list(rows)
    if not rows:
        return 0
    with get_conn() as conn, conn.cursor() as cur:
        cur.executemany(UPSERT_SQL, rows)
        conn.commit()
        return cur.rowcount


def read_latest_snapshot(symbol: str) -> List[Dict]:
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(READ_LAST_TS_SQL, (symbol,))
        tsrow = cur.fetchone()
        if not tsrow:
            return []
        ts = tsrow["ts_ist"]
        cur.execute(READ_SNAPSHOT_SQL, (symbol, ts))
        return cur.fetchall()
