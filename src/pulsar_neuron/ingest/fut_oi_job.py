from __future__ import annotations

import datetime
import logging

from pulsar_neuron.config.kite_auth import load_kite_creds
from pulsar_neuron.config.loader import load_config
from pulsar_neuron.db.fut_oi_repo import upsert_many
from pulsar_neuron.normalize.fut_oi_norm import normalize_fut_oi

log = logging.getLogger(__name__)


def _fetch_live_snapshot(symbols: list[str]) -> list[dict]:
    """Placeholder for live broker API fetch.

    For now this simply returns static mock data. The helper exists to make it
    easier to wire the real Kite call in the next milestone while keeping the
    rest of the job stable.
    """

    # TODO(v0.4): integrate with Kite using ``load_kite_creds``.
    now = datetime.datetime.now(datetime.timezone.utc)
    rows = []
    for sym in symbols:
        rows.append({
            "symbol": sym,
            "ts_ist": now,
            "oi": 1_000_000,
            "price": 100.0,
            "tag": "live",
        })
    return rows


def _generate_mock_snapshot(symbols: list[str]) -> list[dict]:
    now = datetime.datetime.now(datetime.timezone.utc)
    return [{
        "symbol": sym,
        "ts_ist": now,
        "oi": 1_000_000,
        "price": 100.0,
        "tag": "mock",
    } for sym in symbols]


def run(mode: str = "live") -> None:
    """Fetch and store futures open interest snapshots.

    Parameters
    ----------
    mode:
        ``"live"`` (default) - fetch from broker API (stubbed until v0.4).
        ``"mock"`` - insert deterministic placeholder rows, useful for tests.
    """

    cfg = load_config("markets.yaml")
    tokens_cfg = cfg.get("tokens") or {}
    symbols = list(tokens_cfg.keys()) if isinstance(tokens_cfg, dict) else list(tokens_cfg)

    if not symbols:
        log.warning("⚠️ fut_oi_job: no symbols configured in markets.yaml -> tokens")
        return

    if mode == "live":
        try:
            load_kite_creds()  # ensures creds exist; actual usage in v0.4
        except Exception as exc:  # pragma: no cover - defensive log, tests rely on mock
            log.warning("⚠️ fut_oi_job: unable to load Kite creds: %s", exc)
        rows = _fetch_live_snapshot(symbols)
    else:
        rows = _generate_mock_snapshot(symbols)

    normed = [normalize_fut_oi(r) for r in rows]
    upsert_many(normed)
    log.info("✅ fut_oi_job: inserted %d rows", len(normed))


if __name__ == "__main__":
    run("mock")
