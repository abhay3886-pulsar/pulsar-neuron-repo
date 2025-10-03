CREATE TABLE IF NOT EXISTS fut_oi_baseline (
  symbol TEXT NOT NULL,
  trading_day DATE NOT NULL,
  ts_ist TIMESTAMPTZ NOT NULL,
  price NUMERIC NOT NULL,
  oi BIGINT NOT NULL,
  PRIMARY KEY (symbol, trading_day)
);
CREATE TABLE IF NOT EXISTS ib_snapshot (
  symbol TEXT NOT NULL,
  trading_day DATE NOT NULL,
  ib_start TIME NOT NULL,
  ib_end TIME NOT NULL,
  ib_high NUMERIC NOT NULL,
  ib_low NUMERIC NOT NULL,
  PRIMARY KEY (symbol, trading_day)
);
