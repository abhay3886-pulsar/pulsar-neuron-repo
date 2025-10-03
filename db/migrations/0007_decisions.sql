CREATE TABLE IF NOT EXISTS decisions (
  symbol TEXT NOT NULL,
  ts_ist TIMESTAMPTZ NOT NULL,
  action TEXT NOT NULL, -- 'long' | 'short' | 'wait'
  confidence INTEGER NOT NULL,
  chosen_strategy TEXT,
  reasons JSONB NOT NULL,
  bull_case JSONB NOT NULL,
  bear_case JSONB NOT NULL,
  overrides JSONB NOT NULL,
  context_ref TIMESTAMPTZ NOT NULL,
  PRIMARY KEY (symbol, ts_ist)
);
