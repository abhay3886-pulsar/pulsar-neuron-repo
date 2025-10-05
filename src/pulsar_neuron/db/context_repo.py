from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, Optional
from zoneinfo import ZoneInfo

from .postgres import get_conn

IST = ZoneInfo("Asia/Kolkata")

# --------------------------------------------------------------------------------------
# SQL
# --------------------------------------------------------------------------------------

UPSERT_SQL = """
             INSERT INTO context(symbol, ts_ist, data, meta)
             VALUES (%(symbol)s, %(ts_ist)s, %(data)s, %(meta)s)
             ON CONFLICT (symbol, ts_ist) DO UPDATE SET data = EXCLUDED.data,
                                                        meta = EXCLUDED.meta; \
             """

READ_LAST_SQL = """
                SELECT symbol, ts_ist, data, meta
                FROM context
                WHERE symbol = %s
                ORDER BY ts_ist DESC
                LIMIT 1; \
                """


# --------------------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------------------

def _ensure_ist(dt: datetime) -> datetime:
    """Return tz-aware datetime in Asia/Kolkata."""
    if dt.tzinfo is None:
        return dt.replace(tzinfo=IST)
    return dt.astimezone(IST)


# --------------------------------------------------------------------------------------
# Public API
# --------------------------------------------------------------------------------------

def insert_context(row: Dict[str, Any]) -> int:
    """
    Insert or update a context snapshot.

    Expected keys:
      - symbol: str
      - ts_ist: datetime (naive or tz-aware)
      - data: JSON/dict (serialized automatically by psycopg2)
      - meta: JSON/dict or None
    """
    row = dict(row)
    if "ts_ist" in row and isinstance(row["ts_ist"], datetime):
        row["ts_ist"] = _ensure_ist(row["ts_ist"])

    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(UPSERT_SQL, row)
        conn.commit()
        return cur.rowcount


def latest_context(symbol: str) -> Optional[Dict[str, Any]]:
    """
    Return the latest context snapshot for a symbol as dict, or None if not found.
    """
    from psycopg2.extras import RealDictCursor  # type: ignore
    with get_conn() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(READ_LAST_SQL, (symbol,))
        row = cur.fetchone()
        if not row:
            return None
        ts = row.get("ts_ist")
        if isinstance(ts, datetime):
            row["ts_ist"] = _ensure_ist(ts)
        return dict(row)
