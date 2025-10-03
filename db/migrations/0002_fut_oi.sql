CREATE TABLE IF NOT EXISTS fut_oi (
  symbol TEXT NOT NULL,
  ts_ist TIMESTAMPTZ NOT NULL,
  price NUMERIC NOT NULL,
  oi BIGINT NOT NULL,
  baseline_tag TEXT, -- 'open_baseline' | 'intraday' | 'close'
  PRIMARY KEY (symbol, ts_ist)
);
