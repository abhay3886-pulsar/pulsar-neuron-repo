from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

from zoneinfo import ZoneInfo

# Uses your project's connection helper that returns a psycopg2 connection
from .postgres import get_conn

try:
    # These are available with psycopg2
    from psycopg2.extras import RealDictCursor, execute_values  # type: ignore
    _HAVE_PSYCOPG2_EXTRAS = True
except Exception:  # pragma: no cover
    _HAVE_PSYCOPG2_EXTRAS = False

IST = ZoneInfo("Asia/Kolkata")

__all__ = [
    "upsert_many",
    "read_last_n",
    "read_range",
    "read_range_semi_open",
    "get_max_ts",
    "read_last_complete_before",
]

# --------------------------------------------------------------------------------------
# SQL
# --------------------------------------------------------------------------------------

UPSERT_SQL = """
INSERT INTO ohlcv(symbol, ts_ist, tf, o, h, l, c, v)
VALUES %s
ON CONFLICT (symbol, ts_ist, tf) DO UPDATE SET
  o = EXCLUDED.o,
  h = EXCLUDED.h,
  l = EXCLUDED.l,
  c = EXCLUDED.c,
  v = EXCLUDED.v;
"""

READ_LAST_N_SQL = """
SELECT symbol, ts_ist, tf, o, h, l, c, v
FROM ohlcv
WHERE symbol = %s AND tf = %s
ORDER BY ts_ist DESC
LIMIT %s
"""

READ_RANGE_SQL_CLOSED = """
SELECT symbol, ts_ist, tf, o, h, l, c, v
FROM ohlcv
WHERE symbol = %s
  AND tf = %s
  AND ts_ist >= %s
  AND ts_ist <= %s
ORDER BY ts_ist ASC
"""

READ_RANGE_SQL_SEMI_OPEN = """
SELECT symbol, ts_ist, tf, o, h, l, c, v
FROM ohlcv
WHERE symbol = %s
  AND tf = %s
  AND ts_ist >= %s
  AND ts_ist < %s
ORDER BY ts_ist ASC
"""

READ_MAX_TS_SQL = """
SELECT MAX(ts_ist) AS ts_ist
FROM ohlcv
WHERE symbol = %s AND tf = %s
"""

READ_LAST_COMPLETE_BEFORE_SQL = """
-- “Complete bar” = strictly before the boundary timestamp
SELECT symbol, ts_ist, tf, o, h, l, c, v
FROM ohlcv
WHERE symbol = %s AND tf = %s AND ts_ist < %s
ORDER BY ts_ist DESC
LIMIT 1
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
    """Map incoming row mappings to VALUES tuples in column order used in UPSERT_SQL."""
    vals: List[Tuple[Any, ...]] = []
    for r in rows:
        vals.append((
            r["symbol"],
            _ensure_ist(r["ts_ist"]),
            r["tf"],
            r["o"], r["h"], r["l"], r["c"], r["v"],
        ))
    return vals


# --------------------------------------------------------------------------------------
# Public API (backwards compatible)
# --------------------------------------------------------------------------------------

def upsert_many(rows: Iterable[Mapping[str, Any]], batch_size: int = 2000) -> int:
    """
    Fast, safe upsert using INSERT .. ON CONFLICT.
    Returns the number of rows *sent* to the DB (len(rows)).

    Backwards compatible with the original signature/semantics (int return).
    """
    rows_list = list(rows)
    if not rows_list:
        return 0

    values = _values_from_rows(rows_list)

    with get_conn() as conn, conn.cursor() as cur:
        if _HAVE_PSYCOPG2_EXTRAS:
            # Use execute_values for bulk speed
            template = "(%s,%s,%s,%s,%s,%s,%s,%s)"
            execute_values(cur, UPSERT_SQL, values, template=template, page_size=batch_size)
        else:
            # Fallback to executemany with single-row VALUES for environments without extras
            single_sql = """
            INSERT INTO ohlcv(symbol, ts_ist, tf, o, h, l, c, v)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (symbol, ts_ist, tf) DO UPDATE SET
              o = EXCLUDED.o,
              h = EXCLUDED.h,
              l = EXCLUDED.l,
              c = EXCLUDED.c,
              v = EXCLUDED.v;
            """
            cur.executemany(single_sql, values)
        conn.commit()
    return len(rows_list)


def read_last_n(symbol: str, tf: str, n: int) -> List[Dict[str, Any]]:
    """
    Return the last N bars for (symbol, tf) in ascending ts_ist order.
    """
    with get_conn() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:  # type: ignore[arg-type]
        cur.execute(READ_LAST_N_SQL, (symbol, tf, n))
        rows = cur.fetchall()  # newest→oldest
        rows.reverse()         # ascending
        return [dict(r) for r in rows]


def read_range(symbol: str, tf: str, start: datetime, end: datetime) -> List[Dict[str, Any]]:
    """
    Closed interval [start, end].
    Preserves original signature/behavior.
    """
    start_ist = _ensure_ist(start)
    end_ist = _ensure_ist(end)
    with get_conn() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:  # type: ignore[arg-type]
        cur.execute(READ_RANGE_SQL_CLOSED, (symbol, tf, start_ist, end_ist))
        rows = cur.fetchall()
        return [dict(r) for r in rows]


# --------------------------------------------------------------------------------------
# Useful extras (optional but production-friendly)
# --------------------------------------------------------------------------------------

def read_range_semi_open(symbol: str, tf: str, start: datetime, end: datetime) -> List[Dict[str, Any]]:
    """
    Semi-open interval [start, end) — handy for bar-complete windows.
    """
    start_ist = _ensure_ist(start)
    end_ist = _ensure_ist(end)
    with get_conn() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:  # type: ignore[arg-type]
        cur.execute(READ_RANGE_SQL_SEMI_OPEN, (symbol, tf, start_ist, end_ist))
        rows = cur.fetchall()
        return [dict(r) for r in rows]


def get_max_ts(symbol: str, tf: str) -> Optional[datetime]:
    """
    Returns the latest ts_ist for (symbol, tf) or None if no data.
    """
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(READ_MAX_TS_SQL, (symbol, tf))
        row = cur.fetchone()
        ts = row[0] if row and row[0] is not None else None
        return ts.astimezone(IST) if isinstance(ts, datetime) else ts


def read_last_complete_before(symbol: str, tf: str, boundary: datetime) -> Optional[Dict[str, Any]]:
    """
    Returns the last *complete* bar strictly before `boundary`.
    Useful when you receive a new (possibly in-progress) bar timestamp and
    want the previous, fully closed bar for decisions.
    """
    boundary_ist = _ensure_ist(boundary)
    with get_conn() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:  # type: ignore[arg-type]
        cur.execute(READ_LAST_COMPLETE_BEFORE_SQL, (symbol, tf, boundary_ist))
        row = cur.fetchone()
        return dict(row) if row else None
