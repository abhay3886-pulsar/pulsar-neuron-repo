"""Futures OI repository — UPSERT and reads."""
from __future__ import annotations

from datetime import datetime
from typing import Dict, Iterable, List, Mapping

from .postgres import get_conn

UPSERT_SQL = """
insert into fut_oi(symbol, ts_ist, price, oi, tag)
values (%(symbol)s, %(ts_ist)s, %(price)s, %(oi)s, %(tag)s)
on conflict (symbol, ts_ist) do update
set price = excluded.price, oi = excluded.oi, tag = excluded.tag;
"""

READ_LAST_SQL = """
select symbol, ts_ist, price, oi, tag
from fut_oi
where symbol = %s
order by ts_ist desc
limit %s
"""

READ_RANGE_SQL = """
select symbol, ts_ist, price, oi, tag
from fut_oi
where symbol = %s and ts_ist between %s and %s
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


def read_last(symbol: str, limit: int = 50) -> List[Dict]:
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(READ_LAST_SQL, (symbol, limit))
        return cur.fetchall()


def read_range(symbol: str, start: datetime, end: datetime) -> List[Dict]:
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(READ_RANGE_SQL, (symbol, start, end))
        return cur.fetchall()
