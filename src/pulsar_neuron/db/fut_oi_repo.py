from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, Iterable, List, Mapping, Optional, Tuple

from zoneinfo import ZoneInfo
from .postgres import get_conn

try:
    from psycopg2.extras import RealDictCursor, execute_values  # type: ignore
    _HAVE_EXTRAS = True
except Exception:  # pragma: no cover
    _HAVE_EXTRAS = False

IST = ZoneInfo("Asia/Kolkata")

__all__ = [
    "upsert_many",
    "read_last",
    # optional helpers
    "get_latest_ts",
    "read_between",
    "read_last_by_tag",
]

# --------------------------------------------------------------------------------------
# SQL
# --------------------------------------------------------------------------------------

UPSERT_SQL = """
INSERT INTO fut_oi(symbol, ts_ist, price, oi, tag)
VALUES %s
ON CONFLICT (symbol, ts_ist) DO UPDATE SET
  price = EXCLUDED.price,
  oi    = EXCLUDED.oi,
  tag   = EXCLUDED.tag;
"""

READ_LAST_SQL = """
SELECT symbol, ts_ist, price, oi, tag
FROM fut_oi
WHERE symbol = %s
ORDER BY ts_ist DESC
LIMIT %s
"""

READ_BETWEEN_SQL = """
SELECT symbol, ts_ist, price, oi, tag
FROM fut_oi
WHERE symbol = %s AND ts_ist >= %s AND ts_ist <= %s
ORDER BY ts_ist ASC
"""

READ_LAST_BY_TAG_SQL = """
SELECT symbol, ts_ist, price, oi, tag
FROM fut_oi
WHERE symbol = %s AND tag = %s
ORDER BY ts_ist DESC
LIMIT %s
"""

READ_MAX_TS_SQL = """
SELECT MAX(ts_ist) AS ts_ist
FROM fut_oi
WHERE symbol = %s
"""

# --------------------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------------------

def _ensure_ist(dt: datetime) -> datetime:
    """Return tz-aware datetime in IST (convert if naive or other tz)."""
    if dt.tzinfo is None:
        return dt.replace(tzinfo=IST)
    return dt.astimezone(IST)

def _values_from_rows(rows: Iterable[Mapping[str, Any]]) -> List[Tuple[Any, ...]]:
    vals: List[Tuple[Any, ...]] = []
    for r in rows:
        vals.append((
            r["symbol"],
            _ensure_ist(r["ts_ist"]),
            r["price"],
            r["oi"],
            r["tag"],   # e.g., 'open_baseline' | 'intraday' | 'close'
        ))
    return vals

# --------------------------------------------------------------------------------------
# Public API (backwards compatible)
# --------------------------------------------------------------------------------------

def upsert_many(rows: Iterable[Mapping]) -> int:
    """
    Fast batch upsert for fut_oi.
    Returns number of rows sent (len(rows)), matching original return type.
    """
    rows_list = list(rows)
    if not rows_list:
        return 0

    values = _values_from_rows(rows_list)

    with get_conn() as conn, conn.cursor() as cur:
        if _HAVE_EXTRAS:
            template = "(%s,%s,%s,%s,%s)"
            execute_values(cur, UPSERT_SQL, values, template=template, page_size=2000)
        else:
            single_sql = """
            INSERT INTO fut_oi(symbol, ts_ist, price, oi, tag)
            VALUES (%s,%s,%s,%s,%s)
            ON CONFLICT (symbol, ts_ist) DO UPDATE SET
              price = EXCLUDED.price,
              oi    = EXCLUDED.oi,
              tag   = EXCLUDED.tag;
            """
            cur.executemany(single_sql, values)
        conn.commit()
    return len(rows_list)

def read_last(symbol: str, limit: int = 50) -> List[Dict]:
    """
    EXACT signature preserved.
    Returns last N rows as list[dict] in DESC ts_ist order (newest first).
    """
    with get_conn() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:  # type: ignore[arg-type]
        cur.execute(READ_LAST_SQL, (symbol, limit))
        rows = cur.fetchall()
        # Normalize ts_ist to IST on read as well
        for r in rows:
            ts = r.get("ts_ist")
            if isinstance(ts, datetime):
                r["ts_ist"] = _ensure_ist(ts)
        return [dict(r) for r in rows]

# --------------------------------------------------------------------------------------
# Optional helpers (non-breaking)
# --------------------------------------------------------------------------------------

def get_latest_ts(symbol: str) -> Optional[datetime]:
    """Return latest ts_ist for symbol in IST, or None."""
    with get_conn() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:  # type: ignore[arg-type]
        cur.execute(READ_MAX_TS_SQL, (symbol,))
        row = cur.fetchone()
        ts = row["ts_ist"] if row else None
        return _ensure_ist(ts) if isinstance(ts, datetime) else None

def read_between(symbol: str, start: datetime, end: datetime) -> List[Dict]:
    """Return rows in closed interval [start, end] in ASC order."""
    s_ist = _ensure_ist(start)
    e_ist = _ensure_ist(end)
    with get_conn() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:  # type: ignore[arg-type]
        cur.execute(READ_BETWEEN_SQL, (symbol, s_ist, e_ist))
        rows = cur.fetchall()
        for r in rows:
            ts = r.get("ts_ist")
            if isinstance(ts, datetime):
                r["ts_ist"] = _ensure_ist(ts)
        return [dict(r) for r in rows]

def read_last_by_tag(symbol: str, tag: str, limit: int = 1) -> List[Dict]:
    """Return latest rows for a specific tag (e.g., 'open_baseline') in DESC order."""
    with get_conn() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:  # type: ignore[arg-type]
        cur.execute(READ_LAST_BY_TAG_SQL, (symbol, tag, limit))
        rows = cur.fetchall()
        for r in rows:
            ts = r.get("ts_ist")
            if isinstance(ts, datetime):
                r["ts_ist"] = _ensure_ist(ts)
        return [dict(r) for r in rows]
