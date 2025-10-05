# src/pulsar_neuron/db/market_repo.py
from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, Mapping, Optional

from zoneinfo import ZoneInfo
from .postgres import get_conn

try:
    from psycopg2.extras import RealDictCursor  # type: ignore
    _HAVE_REALDICT = True
except Exception:  # pragma: no cover
    RealDictCursor = None  # type: ignore[assignment]
    _HAVE_REALDICT = False

IST = ZoneInfo("Asia/Kolkata")

__all__ = [
    "upsert_one",
    "read_latest",
    # optional helpers
    "get_latest_ts",
    "read_latest_as_tuple",
]

# --------------------------------------------------------------------------------------
# SQL (combined breadth + vix in one table)
# Table: market_breadth(ts_ist TIMESTAMPTZ PRIMARY KEY, adv INT, dec INT, unch INT, vix DOUBLE PRECISION)
# --------------------------------------------------------------------------------------

UPSERT_SQL = """
INSERT INTO market_breadth(ts_ist, adv, dec, unch, vix)
VALUES (%(ts_ist)s, %(adv)s, %(dec)s, %(unch)s, %(vix)s)
ON CONFLICT (ts_ist) DO UPDATE SET
  adv  = EXCLUDED.adv,
  dec  = EXCLUDED.dec,
  unch = EXCLUDED.unch,
  vix  = EXCLUDED.vix;
"""

READ_LAST_SQL = """
SELECT ts_ist, adv, dec, unch, vix
FROM market_breadth
ORDER BY ts_ist DESC
LIMIT 1
"""

READ_LAST_TS_SQL = """
SELECT MAX(ts_ist) AS ts_ist
FROM market_breadth
"""

# --------------------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------------------

def _ensure_ist(dt: datetime) -> datetime:
    """Ensure tz-aware IST."""
    if dt.tzinfo is None:
        return dt.replace(tzinfo=IST)
    return dt.astimezone(IST)

def _dictify_one(cur, row):
    """Convert a tuple row to dict using cursor.description."""
    if row is None:
        return None
    cols = [d[0] for d in cur.description]
    return dict(zip(cols, row))

# --------------------------------------------------------------------------------------
# Public API
# --------------------------------------------------------------------------------------

def upsert_one(row: Mapping[str, Any]) -> int:
    """
    Upsert a single breadth snapshot (ts_ist, adv, dec, unch, vix).
    Returns affected row count (1 on success).
    """
    # Normalize ts_ist to IST to avoid mixed tz in the table.
    ts = row.get("ts_ist")
    if isinstance(ts, datetime):
        row = dict(row)  # copy to mutate safely
        row["ts_ist"] = _ensure_ist(ts)

    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(UPSERT_SQL, row)
        # Connection context manager will commit on successful exit.
        return cur.rowcount or 1

def read_latest() -> Optional[Dict[str, Any]]:
    """
    Return the most recent breadth point as a dict:
    {ts_ist(IST), adv, dec, unch, vix} or None if table is empty.
    """
    cursor_factory = RealDictCursor if _HAVE_REALDICT else None
    with get_conn() as conn, conn.cursor(cursor_factory=cursor_factory) as cur:  # type: ignore[arg-type]
        cur.execute(READ_LAST_SQL)
        row = cur.fetchone()
        if not row:
            return None
        row = row if _HAVE_REALDICT else _dictify_one(cur, row)
        # Normalize ts to IST on read as well.
        ts = row.get("ts_ist")
        if isinstance(ts, datetime):
            row["ts_ist"] = _ensure_ist(ts)
        return row

# --------------------------------------------------------------------------------------
# Optional helpers
# --------------------------------------------------------------------------------------

def get_latest_ts() -> Optional[datetime]:
    """Return the latest ts_ist as IST, or None if table is empty."""
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(READ_LAST_TS_SQL)
        row = cur.fetchone()
        ts = row[0] if row else None
        return _ensure_ist(ts) if isinstance(ts, datetime) else None

def read_latest_as_tuple() -> Optional[tuple]:
    """
    Same as read_latest() but returns a positional tuple:
    (ts_ist[IST], adv, dec, unch, vix)
    """
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(READ_LAST_SQL)
        row = cur.fetchone()
        if not row:
            return None
        ts, adv, dec, unch, vix = row  # matches SELECT order
        if isinstance(ts, datetime):
            ts = _ensure_ist(ts)
        return (ts, adv, dec, unch, vix)
