from __future__ import annotations

import datetime
import logging

from pulsar_neuron.config.loader import load_config
from pulsar_neuron.db.options_repo import upsert_many
from pulsar_neuron.normalize.options_norm import normalize_option_row

log = logging.getLogger(__name__)


def _generate_mock_rows(symbols: list[str]) -> list[dict]:
    now = datetime.datetime.now(datetime.timezone.utc)
    rows: list[dict] = []
    for sym in symbols:
        for side in ("CE", "PE"):
            rows.append({
                "symbol": sym,
                "ts_ist": now,
                "expiry": now.date(),
                "strike": 100.0,
                "side": side,
                "ltp": 1.0,
                "iv": 20.0,
                "oi": 10_000,
                "volume": 500,
                "delta": 0.5,
                "gamma": 0.05,
                "theta": -0.2,
                "vega": 0.1,
            })
    return rows


def run(mode: str = "live") -> None:
    cfg = load_config("markets.yaml")
    tokens_cfg = cfg.get("tokens") or {}
    symbols = list(tokens_cfg.keys()) if isinstance(tokens_cfg, dict) else list(tokens_cfg)

    if not symbols:
        log.warning("⚠️ options_job: no symbols configured in markets.yaml -> tokens")
        return

    if mode == "live":
        # TODO(v0.4): integrate with broker API
        rows = _generate_mock_rows(symbols)
    else:
        rows = _generate_mock_rows(symbols)

    normed = [normalize_option_row(r) for r in rows]
    upsert_many(normed)
    log.info("✅ options_job: inserted %d rows", len(normed))
