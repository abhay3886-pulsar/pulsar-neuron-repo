# ERD — Core Tables
- ohlcv(symbol, ts_ist, tf, o,h,l,c,v) PK(symbol, ts_ist, tf)
- fut_oi(symbol, ts_ist, price, oi, baseline_tag) PK(symbol, ts_ist)
- option_chain(symbol, ts_ist, expiry, strike, side, ltp, iv, oi, doi, vol, delta,gamma,theta,vega) PK(symbol, ts_ist, expiry, strike, side)
- breadth(ts_ist, adv, dec, unchanged) PK(ts_ist)
- vix(ts_ist, value) PK(ts_ist)
- fut_oi_baseline(symbol, trading_day, ts_ist, price, oi) PK(symbol, trading_day)
- ib_snapshot(symbol, trading_day, ib_start, ib_end, ib_high, ib_low) PK(symbol, trading_day)
- context_pack(symbol, ts_ist, ok, payload JSONB) PK(symbol, ts_ist)
- decisions(symbol, ts_ist, action, confidence, reasons, bull_case, bear_case, overrides, context_ref) PK(symbol, ts_ist)
_All timestamps are IST (TIMESTAMPTZ)._
