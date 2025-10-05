from __future__ import annotations

from datetime import datetime
from typing import Dict, Iterable, List, Mapping

from .postgres import get_conn

UPSERT_SQL = """
insert into ohlcv(symbol, ts_ist, tf, o, h, l, c, v)
values (%(symbol)s, %(ts_ist)s, %(tf)s, %(o)s, %(h)s, %(l)s, %(c)s, %(v)s)
on conflict (symbol, ts_ist, tf) do update set
  o = excluded.o, h = excluded.h, l = excluded.l, c = excluded.c, v = excluded.v;
"""

READ_LAST_N_SQL = """
select symbol, ts_ist, tf, o, h, l, c, v
from ohlcv
where symbol = %s and tf = %s
order by ts_ist desc
limit %s
"""

READ_RANGE_SQL = """
select symbol, ts_ist, tf, o, h, l, c, v
from ohlcv
where symbol = %s and tf = %s and ts_ist >= %s and ts_ist <= %s
order by ts_ist asc
"""


def upsert_many(rows: Iterable[Mapping]) -> int:
    rows = list(rows)
    if not rows:
        return 0
    with get_conn() as conn, conn.cursor() as cur:
        cur.executemany(UPSERT_SQL, rows)
        conn.commit()
        return cur.rowcount


def read_last_n(symbol: str, tf: str, n: int) -> List[Dict]:
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(READ_LAST_N_SQL, (symbol, tf, n))
        rows = cur.fetchall()
        return list(reversed(rows))  # ascending


def read_range(symbol: str, tf: str, start: datetime, end: datetime) -> List[Dict]:
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(READ_RANGE_SQL, (symbol, tf, start, end))
        return cur.fetchall()
