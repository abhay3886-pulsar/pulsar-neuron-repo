from __future__ import annotations
import datetime, logging
from typing import List, Dict
from pulsar_neuron.db.options_repo import upsert_many
from pulsar_neuron.normalize.options_norm import normalize_option_row
from pulsar_neuron.config.loader import load_config
from pulsar_neuron.service.kite_client import KiteRest

log = logging.getLogger(__name__)


def _rows_from_quotes(symbol: str, now, tokens: List[int], quotes: Dict) -> list[dict]:
    rows = []
    for tok in tokens:
        q = quotes.get(str(tok)) or quotes.get(tok) or {}
        # We can't infer expiry/strike/side without a token→metadata map.
        # Expect the operator to pre-resolve these fields into config soon.
        meta = q.get("instrument_token_meta") or {}  # placeholder if downstream passes
        expiry = meta.get("expiry")
        strike = meta.get("strike")
        side = meta.get("side")  # "CE"/"PE"

        ltp = q.get("last_price") or q.get("last_trade_price") or 0.0
        iv = q.get("iv") or 0.0
        oi = q.get("oi") or 0
        vol = q.get("volume") or q.get("last_quantity") or 0

        if not (expiry and strike and side):
            # Skip until we have metadata mapping; operator can enrich later
            continue

        rows.append({
            "symbol": symbol,
            "ts_ist": now,
            "expiry": expiry,
            "strike": float(strike),
            "side": side,
            "ltp": float(ltp or 0.0),
            "iv": float(iv or 0.0),
            "oi": int(oi or 0),
            "volume": int(vol or 0),
            "delta": 0.0, "gamma": 0.0, "theta": 0.0, "vega": 0.0
        })
    return rows


def run(mode: str = "live") -> None:
    cfg = load_config("markets.yaml")
    derivs = cfg.get("derivs") or {}
    opt_tokens = derivs.get("options_tokens") or {}
    symbols = list(opt_tokens.keys())
    now = datetime.datetime.now(datetime.timezone.utc)

    if mode == "mock":
        rows = []
        for s in symbols:
            rows.extend([{
                "symbol": s, "ts_ist": now, "expiry": now.date(), "strike": 100.0, "side": side,
                "ltp": 1.0, "iv": 20.0, "oi": 10000, "volume": 500, "delta": 0.5, "gamma": 0.05, "theta": -0.2, "vega": 0.1
            } for side in ("CE","PE")])
    else:
        if not opt_tokens:
            log.warning("options_job: no options_tokens in markets.yaml → skipping")
            return
        api = KiteRest()
        # Flatten tokens once for batch quote
        all_tokens: List[int] = []
        for tks in opt_tokens.values():
            all_tokens.extend(tks or [])
        if not all_tokens:
            log.warning("options_job: empty options token list")
            return
        quotes = api.quote(all_tokens)
        rows = []
        for s, tks in opt_tokens.items():
            rows.extend(_rows_from_quotes(s, now, tks or [], quotes))

    normed = [normalize_option_row(r) for r in rows if r]
    if not normed:
        log.info("options_job: nothing to insert (likely missing metadata for options tokens)")
        return
    upsert_many(normed)
    log.info("✅ options_job: inserted %d rows", len(normed))
