from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, Mapping, Optional, List

from zoneinfo import ZoneInfo
from .postgres import get_conn

try:
    from psycopg2.extras import RealDictCursor  # type: ignore
    _HAVE_REALDICT = True
except Exception:  # pragma: no cover
    _HAVE_REALDICT = False

IST = ZoneInfo("Asia/Kolkata")

__all__ = [
    "upsert_one",
    "read_latest",
    # optional helpers
    "get_latest_ts",
    "read_latest_as_tuple",
]

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

def _ensure_ist(dt: datetime) -> datetime:
    """Make sure a datetime is tz-aware in IST."""
    if dt.tzinfo is None:
        return dt.replace(tzinfo=IST)
    return dt.astimezone(IST)

# --------------------------------------------------------------------------------------
# Public API (backwards compatible)
# --------------------------------------------------------------------------------------

def upsert_one(row: Mapping) -> int:
    """
    Upsert a single breadth row. Returns affected row count (1).
    Keeps the original signature.
    """
    # Normalize ts_ist to IST to avoid mixed-tz inserts.
    ts = row.get("ts_ist")
    if isinstance(ts, datetime):
        # Copy into a mutable dict to replace the timestamp safely.
        row = dict(row)
        row["ts_ist"] = _ensure_ist(ts)

    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(UPSERT_SQL, row)
        conn.commit()
        return cur.rowcount  # typically 1

def read_latest() -> Optional[Dict]:
    """
    Return the most recent breadth point as a dict (or None if empty).
    Keeps the original signature and return type.
    """
    cursor_factory = RealDictCursor if _HAVE_REALDICT else None
    with get_conn() as conn, conn.cursor(cursor_factory=cursor_factory) as cur:  # type: ignore[arg-type]
        cur.execute(READ_LAST_SQL)
        row = cur.fetchone()
        if not row:
            return None
        # If tuple cursor was used, convert to dict with column names.
        if not _HAVE_REALDICT:
            # fetchone() as tuple → map columns manually
            colnames = [desc[0] for desc in cur.description]
            row = dict(zip(colnames, row))
        # Normalize ts_ist to IST on read as well.
        ts = row.get("ts_ist")
        if isinstance(ts, datetime):
            row["ts_ist"] = _ensure_ist(ts)
        return row

# --------------------------------------------------------------------------------------
# Optional helpers (nice to have; no behavior change to your API)
# --------------------------------------------------------------------------------------

def get_latest_ts() -> Optional[datetime]:
    """
    Returns the latest ts_ist in IST, or None if table is empty.
    """
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(READ_LAST_TS_SQL)
        row = cur.fetchone()
        ts = row[0] if row else None
        return _ensure_ist(ts) if isinstance(ts, datetime) else None

def read_latest_as_tuple() -> Optional[tuple]:
    """
    Same as read_latest but returns a positional tuple:
    (ts_ist(IST), adv, dec, unch, vix)
    Useful if you prefer tuple unpacking in hot paths.
    """
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(READ_LAST_SQL)
        row = cur.fetchone()
        if not row:
            return None
        # row is a tuple; convert ts to IST
        # Column order matches SELECT list
        ts, adv, dec, unch, vix = row
        if isinstance(ts, datetime):
            ts = _ensure_ist(ts)
        return (ts, adv, dec, unch, vix)
