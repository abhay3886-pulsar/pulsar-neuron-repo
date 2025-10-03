CREATE TABLE IF NOT EXISTS option_chain (
  symbol TEXT NOT NULL,
  ts_ist TIMESTAMPTZ NOT NULL,
  expiry DATE NOT NULL,
  strike INTEGER NOT NULL,
  side TEXT NOT NULL, -- 'CE' | 'PE'
  ltp NUMERIC,
  iv NUMERIC,
  oi BIGINT NOT NULL,
  doi BIGINT NOT NULL,
  vol BIGINT NOT NULL,
  delta NUMERIC, gamma NUMERIC, theta NUMERIC, vega NUMERIC,
  PRIMARY KEY (symbol, ts_ist, expiry, strike, side)
);
CREATE INDEX IF NOT EXISTS option_chain_atm ON option_chain(symbol, ts_ist, expiry, strike);
