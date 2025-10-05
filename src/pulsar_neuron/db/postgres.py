from __future__ import annotations

from contextlib import contextmanager
import logging
import os
from psycopg2.extras import RealDictCursor
from psycopg2.pool import ThreadedConnectionPool
from pulsar_neuron.config.secrets import get_db_credentials

log = logging.getLogger(__name__)

# ---------------------------
# Global pool (per-process)
# ---------------------------
_POOL: ThreadedConnectionPool | None = None

def _dsn_from_secret() -> str:
    cfg = get_db_credentials()
    return (
        f"host={cfg['host']} port={cfg.get('port',5432)} "
        f"dbname={cfg['database']} user={cfg['username']} "
        f"password={cfg['password']} sslmode={cfg.get('sslmode','require')}"
    )

def init_pool(minconn: int | None = None, maxconn: int | None = None) -> None:
    """Initialize global connection pool. Safe to call multiple times."""
    global _POOL
    if _POOL is not None:
        return
    dsn = _dsn_from_secret()
    # defaults (override via env if needed)
    minconn = minconn or int(os.getenv("DB_POOL_MIN", "1"))
    maxconn = maxconn or int(os.getenv("DB_POOL_MAX", "10"))
    _POOL = ThreadedConnectionPool(minconn, maxconn, dsn=dsn, cursor_factory=RealDictCursor)
    log.info("✅ DB pool initialized min=%s max=%s", minconn, maxconn)

def close_pool() -> None:
    """Close and reset the pool (useful for graceful shutdown)."""
    global _POOL
    if _POOL is not None:
        _POOL.closeall()
        _POOL = None
        log.info("🛑 DB pool closed")

@contextmanager
def get_conn():
    """
    Pooled connection context manager.
    - If pool is not initialized yet, initialize lazily.
    - Yields a psycopg2 connection with RealDictCursor as default cursor.
    """
    global _POOL
    if _POOL is None:
        init_pool()
    assert _POOL is not None, "DB pool not initialized"
    conn = _POOL.getconn()
    try:
        yield conn
    finally:
        _POOL.putconn(conn)

# ---------------------------
# Migrations
# ---------------------------
DDL = r"""
-- (tables from your previous DDL stay the same)
create table if not exists ohlcv (
  symbol  text not null,
  ts_ist  timestamptz not null,
  tf      text not null,
  o numeric not null,
  h numeric not null,
  l numeric not null,
  c numeric not null,
  v bigint  not null,
  primary key (symbol, ts_ist, tf)
);
create index if not exists idx_ohlcv_symbol_tf_ts_desc on ohlcv(symbol, tf, ts_ist desc);

create table if not exists fut_oi (
  symbol  text not null,
  ts_ist  timestamptz not null,
  price   numeric not null,
  oi      bigint  not null,
  tag     text default null,
  primary key (symbol, ts_ist)
);
create index if not exists idx_futoi_symbol_ts_desc on fut_oi(symbol, ts_ist desc);

create table if not exists options_chain (
  symbol  text not null,
  ts_ist  timestamptz not null,
  expiry  date not null,
  strike  numeric not null,
  side    text not null,
  ltp     numeric not null,
  iv      numeric,
  oi      bigint,
  volume  bigint,
  delta   numeric,
  gamma   numeric,
  theta   numeric,
  vega    numeric,
  primary key (symbol, ts_ist, expiry, strike, side)
);
create index if not exists idx_opt_symbol_ts on options_chain(symbol, ts_ist desc);

create table if not exists market_breadth (
  ts_ist  timestamptz primary key,
  adv     integer not null,
  dec     integer not null,
  unch    integer not null,
  vix     numeric  not null
);
"""

def migrate() -> None:
    # use a dedicated single connection for DDL
    init_pool()
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(DDL)
            conn.commit()
    log.info("✅ migrations applied (ohlcv, fut_oi, options_chain, market_breadth)")
