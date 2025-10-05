from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, Optional
from zoneinfo import ZoneInfo

from .postgres import get_conn

IST = ZoneInfo("Asia/Kolkata")

UPSERT_SQL = """
             INSERT INTO decisions(symbol, ts_ist, view, reason, confidence, meta)
             VALUES (%(symbol)s, %(ts_ist)s, %(view)s, %(reason)s, %(confidence)s, %(meta)s)
             ON CONFLICT (symbol, ts_ist) DO UPDATE SET view       = EXCLUDED.view,
                                                        reason     = EXCLUDED.reason,
                                                        confidence = EXCLUDED.confidence,
                                                        meta       = EXCLUDED.meta; \
             """

READ_LAST_SQL = """
                SELECT symbol, ts_ist, view, reason, confidence, meta
                FROM decisions
                WHERE symbol = %s
                ORDER BY ts_ist DESC
                LIMIT 1; \
                """


def _ensure_ist(dt: datetime) -> datetime:
    """Ensure timestamp is tz-aware in Asia/Kolkata."""
    if dt.tzinfo is None:
        return dt.replace(tzinfo=IST)
    return dt.astimezone(IST)


def insert_decision(row: Dict[str, Any]) -> int:
    """
    Insert or update a decision row.
    Required keys:
      symbol, ts_ist, view, reason, confidence, meta (meta can be JSON/dict/None)
    """
    row = dict(row)
    if "ts_ist" in row and isinstance(row["ts_ist"], datetime):
        row["ts_ist"] = _ensure_ist(row["ts_ist"])

    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(UPSERT_SQL, row)
        conn.commit()
        return cur.rowcount


def latest_decision(symbol: str) -> Optional[Dict[str, Any]]:
    """
    Return the most recent decision for a symbol, or None if not found.
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
