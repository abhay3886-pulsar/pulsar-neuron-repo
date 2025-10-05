from __future__ import annotations

from typing import Dict, Mapping, Optional

from .postgres import get_conn

UPSERT_SQL = """
insert into market_breadth(ts_ist, adv, dec, unch, vix)
values (%(ts_ist)s, %(adv)s, %(dec)s, %(unch)s, %(vix)s)
on conflict (ts_ist) do update set
  adv = excluded.adv, dec = excluded.dec, unch = excluded.unch, vix = excluded.vix;
"""

READ_LAST_SQL = """
select ts_ist, adv, dec, unch, vix
from market_breadth
order by ts_ist desc
limit 1
"""


def upsert_one(row: Mapping) -> int:
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(UPSERT_SQL, row)
        conn.commit()
        return cur.rowcount


def read_latest() -> Optional[Dict]:
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(READ_LAST_SQL)
        return cur.fetchone()
