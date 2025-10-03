-- OHLCV table (one table, tf column)
CREATE TABLE IF NOT EXISTS ohlcv (
  symbol TEXT NOT NULL,
  ts_ist TIMESTAMPTZ NOT NULL, -- IST
  tf TEXT NOT NULL,            -- '5m' | '15m' | '1d' | '4h'
  o NUMERIC NOT NULL,
  h NUMERIC NOT NULL,
  l NUMERIC NOT NULL,
  c NUMERIC NOT NULL,
  v BIGINT NOT NULL,
  PRIMARY KEY (symbol, ts_ist, tf)
);
-- Rule: write only after bar close (+ ~10s)
