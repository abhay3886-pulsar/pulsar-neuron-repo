from __future__ import annotations

from contextlib import contextmanager
import logging
import psycopg
from psycopg.rows import dict_row

from pulsar_neuron.config.secrets import get_db_credentials

log = logging.getLogger(__name__)

DDL = r"""
-- OHLCV (5m/15m/1d)
create table if not exists ohlcv (
  symbol  text not null,
  ts_ist  timestamptz not null,   -- IST bar end
  tf      text not null,          -- '5m'|'15m'|'1d'
  o numeric not null,
  h numeric not null,
  l numeric not null,
  c numeric not null,
  v bigint  not null,
  primary key (symbol, ts_ist, tf)
);
create index if not exists idx_ohlcv_symbol_tf_ts_desc on ohlcv(symbol, tf, ts_ist desc);

-- Futures OI
create table if not exists fut_oi (
  symbol  text not null,
  ts_ist  timestamptz not null,
  price   numeric not null,
  oi      bigint  not null,
  tag     text default null,    -- 'open_baseline' | 'intraday' | 'close' (optional)
  primary key (symbol, ts_ist)
);
create index if not exists idx_futoi_symbol_ts_desc on fut_oi(symbol, ts_ist desc);

-- Options Chain (snapshot table)
create table if not exists options_chain (
  symbol  text not null,
  ts_ist  timestamptz not null,
  expiry  date not null,
  strike  numeric not null,
  side    text not null,        -- 'CE'|'PE'
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

-- Market breadth & VIX (coarse context)
create table if not exists market_breadth (
  ts_ist  timestamptz primary key,
  adv     integer not null,
  dec     integer not null,
  unch    integer not null,
  vix     numeric  not null
);
"""


def _dsn_from_secret() -> str:
    cfg = get_db_credentials()
    return (
        f"host={cfg['host']} port={cfg.get('port',5432)} "
        f"dbname={cfg['dbname']} user={cfg['user']} "
        f"password={cfg['password']} sslmode={cfg.get('sslmode','require')}"
    )


@contextmanager
def get_conn():
    conn = psycopg.connect(_dsn_from_secret(), row_factory=dict_row)
    try:
        yield conn
    finally:
        conn.close()


def migrate() -> None:
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(DDL)
        conn.commit()
        log.info("✅ migrations applied (ohlcv, fut_oi, options_chain, market_breadth)")
