from __future__ import annotations
import logging, datetime
from typing import List, Dict
from pulsar_neuron.db.fut_oi_repo import upsert_many
from pulsar_neuron.normalize.fut_oi_norm import normalize_fut_oi
from pulsar_neuron.config.loader import load_config
from pulsar_neuron.service.kite_client import KiteRest

log = logging.getLogger(__name__)


def _build_rows_from_quote(now, symbols: List[str], fut_tokens: Dict[str, int], quotes: Dict) -> list[dict]:
    rows = []
    for s in symbols:
        tok = fut_tokens.get(s)
        if not tok:
            continue
        q = quotes.get(str(tok)) or quotes.get(tok) or {}
        oi = q.get("oi") or q.get("last_quantity") or 0
        last_price = q.get("last_price") or q.get("last_trade_price") or 0.0
        rows.append({"symbol": s, "ts_ist": now, "oi": int(oi or 0), "price": float(last_price or 0.0)})
    return rows


def run(mode: str = "live") -> None:
    cfg = load_config("markets.yaml")
    symbols = list(cfg.get("tokens") or {})
    now = datetime.datetime.now(datetime.timezone.utc)

    if mode == "mock":
        rows = [{"symbol": s, "ts_ist": now, "oi": 1000000, "price": 100.0} for s in symbols]
    else:
        derivs = cfg.get("derivs") or {}
        fut_tokens = derivs.get("futures_tokens") or {}
        if not fut_tokens:
            log.warning("fut_oi_job: no futures_tokens in markets.yaml → skipping")
            return
        api = KiteRest()
        # quote returns dict keyed by token string
        quotes = api.quote(list(fut_tokens.values()))
        rows = _build_rows_from_quote(now, symbols, fut_tokens, quotes)

    normed = [normalize_fut_oi(r) for r in rows if r]
    if not normed:
        log.info("fut_oi_job: nothing to insert")
        return
    upsert_many(normed)
    log.info("✅ fut_oi_job: inserted %d rows", len(normed))


if __name__ == "__main__":
    run("mock")
