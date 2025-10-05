from __future__ import annotations

import os
from datetime import datetime, date
from typing import Any, Dict, Iterable, List, Mapping, Optional, Tuple

from zoneinfo import ZoneInfo
from .postgres import get_conn

try:
    from psycopg2.extras import RealDictCursor, execute_values  # type: ignore
    _HAVE_EXTRAS = True
except Exception:  # pragma: no cover
    RealDictCursor = None  # type: ignore[assignment]
    execute_values = None  # type: ignore[assignment]
    _HAVE_EXTRAS = False

IST = ZoneInfo("Asia/Kolkata")

# ---- Table name: default to singular; override with PULSAR_OPTIONS_TABLE if your DB uses plural.
TABLE = os.getenv("PULSAR_OPTIONS_TABLE", "option_chain")

__all__ = [
    "upsert_many",
    "read_latest_snapshot",
    # extras
    "get_latest_ts",
    "read_snapshot",
    "read_snapshot_by_expiry",
]

# --------------------------------------------------------------------------------------
# SQL (use TABLE variable)
# --------------------------------------------------------------------------------------

UPSERT_SQL = f"""
INSERT INTO {TABLE}(
  symbol, ts_ist, expiry, strike, side, ltp, iv, oi, volume, delta, gamma, theta, vega
)
VALUES %s
ON CONFLICT (symbol, ts_ist, expiry, strike, side) DO UPDATE SET
  ltp    = EXCLUDED.ltp,
  iv     = EXCLUDED.iv,
  oi     = EXCLUDED.oi,
  volume = EXCLUDED.volume,
  delta  = EXCLUDED.delta,
  gamma  = EXCLUDED.gamma,
  theta  = EXCLUDED.theta,
  vega   = EXCLUDED.vega;
"""

READ_LAST_TS_SQL = f"""
SELECT MAX(ts_ist) AS ts_ist
FROM {TABLE}
WHERE symbol = %s
"""

READ_SNAPSHOT_SQL = f"""
SELECT symbol, ts_ist, expiry, strike, side, ltp, iv, oi, volume, delta, gamma, theta, vega
FROM {TABLE}
WHERE symbol = %s AND ts_ist = %s
ORDER BY strike ASC, side ASC
"""

READ_SNAPSHOT_BY_EXPIRY_SQL = f"""
SELECT symbol, ts_ist, expiry, strike, side, ltp, iv, oi, volume, delta, gamma, theta, vega
FROM {TABLE}
WHERE symbol = %s AND ts_ist = %s AND expiry = %s
ORDER BY strike ASC, side ASC
"""

# --------------------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------------------

def _ensure_ist(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=IST)
    return dt.astimezone(IST)

def _values_from_rows(rows: Iterable[Mapping[str, Any]]) -> List[Tuple[Any, ...]]:
    vals: List[Tuple[Any, ...]] = []
    for r in rows:
        exp = r["expiry"]
        # Coerce expiry to date if a datetime or string "YYYY-MM-DD"
        if isinstance(exp, datetime):
            exp = exp.date()
        elif isinstance(exp, str):
            exp = date.fromisoformat(exp)
        vals.append((
            r["symbol"],
            _ensure_ist(r["ts_ist"]),
            exp,
            r["strike"],
            r["side"],      # 'CE' | 'PE'
            r["ltp"],
            r.get("iv"),
            r.get("oi"),
            r.get("volume"),
            r.get("delta"),
            r.get("gamma"),
            r.get("theta"),
            r.get("vega"),
        ))
    return vals

def _dictify_many(cur, rows):
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, row)) for row in rows]

# --------------------------------------------------------------------------------------
# Public API
# --------------------------------------------------------------------------------------

def upsert_many(rows: Iterable[Mapping]) -> int:
    """
    Fast batch upsert.
    Returns number of rows sent (compat with your original return type).
    """
    rows_list = list(rows)
    if not rows_list:
        return 0

    values = _values_from_rows(rows_list)

    with get_conn() as conn, conn.cursor() as cur:
        if _HAVE_EXTRAS and execute_values is not None:
            template = "(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
            execute_values(cur, UPSERT_SQL, values, template=template, page_size=2000)
        else:
            single = f"""
            INSERT INTO {TABLE}(
              symbol, ts_ist, expiry, strike, side, ltp, iv, oi, volume, delta, gamma, theta, vega
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (symbol, ts_ist, expiry, strike, side) DO UPDATE SET
              ltp=EXCLUDED.ltp, iv=EXCLUDED.iv, oi=EXCLUDED.oi, volume=EXCLUDED.volume,
              delta=EXCLUDED.delta, gamma=EXCLUDED.gamma, theta=EXCLUDED.theta, vega=EXCLUDED.vega;
            """
            cur.executemany(single, values)
        conn.commit()
    return len(rows_list)


def read_latest_snapshot(symbol: str) -> List[Dict]:
    """
    EXACT signature preserved.
    Returns the latest full chain snapshot (all strikes/sides) as list[dict].
    """
    with get_conn() as conn:
        if _HAVE_EXTRAS and RealDictCursor is not None:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:  # type: ignore[arg-type]
                cur.execute(READ_LAST_TS_SQL, (symbol,))
                tsrow = cur.fetchone()
                if not tsrow or not tsrow["ts_ist"]:
                    return []
                ts = _ensure_ist(tsrow["ts_ist"])
                cur.execute(READ_SNAPSHOT_SQL, (symbol, ts))
                rows = cur.fetchall()
                return [dict(r) for r in rows]
        else:
            with conn.cursor() as cur:
                cur.execute(READ_LAST_TS_SQL, (symbol,))
                tsrow = cur.fetchone()
                if not tsrow or not tsrow[0]:
                    return []
                ts = _ensure_ist(tsrow[0])
                cur.execute(READ_SNAPSHOT_SQL, (symbol, ts))
                rows = cur.fetchall()
                return _dictify_many(cur, rows)

# --------------------------------------------------------------------------------------
# Useful extras
# --------------------------------------------------------------------------------------

def get_latest_ts(symbol: str) -> Optional[datetime]:
    with get_conn() as conn:
        if _HAVE_EXTRAS and RealDictCursor is not None:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:  # type: ignore[arg-type]
                cur.execute(READ_LAST_TS_SQL, (symbol,))
                row = cur.fetchone()
                ts = row["ts_ist"] if row else None
        else:
            with conn.cursor() as cur:
                cur.execute(READ_LAST_TS_SQL, (symbol,))
                row = cur.fetchone()
                ts = row[0] if row else None
    return _ensure_ist(ts) if isinstance(ts, datetime) else None


def read_snapshot(symbol: str, ts_ist: datetime) -> List[Dict]:
    ts = _ensure_ist(ts_ist)
    with get_conn() as conn:
        if _HAVE_EXTRAS and RealDictCursor is not None:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:  # type: ignore[arg-type]
                cur.execute(READ_SNAPSHOT_SQL, (symbol, ts))
                rows = cur.fetchall()
                return [dict(r) for r in rows]
        else:
            with conn.cursor() as cur:
                cur.execute(READ_SNAPSHOT_SQL, (symbol, ts))
                rows = cur.fetchall()
                return _dictify_many(cur, rows)


def read_snapshot_by_expiry(symbol: str, ts_ist: datetime, expiry: datetime | date | str) -> List[Dict]:
    ts = _ensure_ist(ts_ist)
    # Normalize expiry to date
    if isinstance(expiry, datetime):
        exp = expiry.date()
    elif isinstance(expiry, str):
        exp = date.fromisoformat(expiry)
    else:
        exp = expiry  # already date
    with get_conn() as conn:
        if _HAVE_EXTRAS and RealDictCursor is not None:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:  # type: ignore[arg-type]
                cur.execute(READ_SNAPSHOT_BY_EXPIRY_SQL, (symbol, ts, exp))
                rows = cur.fetchall()
                return [dict(r) for r in rows]
        else:
            with conn.cursor() as cur:
                cur.execute(READ_SNAPSHOT_BY_EXPIRY_SQL, (symbol, ts, exp))
                rows = cur.fetchall()
                return _dictify_many(cur, rows)
