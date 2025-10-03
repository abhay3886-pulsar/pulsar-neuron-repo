CREATE TABLE IF NOT EXISTS context_pack (
  symbol TEXT NOT NULL,
  ts_ist TIMESTAMPTZ NOT NULL,
  ok BOOLEAN NOT NULL,
  payload JSONB NOT NULL,
  PRIMARY KEY (symbol, ts_ist)
);
