"""Provider factory helpers."""

from __future__ import annotations

import logging
import os
from typing import Any, Mapping

from pulsar_neuron.config.loader import load_defaults
from pulsar_neuron.providers.market_provider import MarketProvider
from pulsar_neuron.providers.mock_provider import MockMarketProvider

try:  # pragma: no cover - optional dependency in tests
    from pulsar_neuron.providers.kite_provider import KiteMarketProvider
except Exception:  # pragma: no cover - kite optional
    KiteMarketProvider = None  # type: ignore[assignment]


def resolve_provider(
    config: Mapping[str, Any] | None = None,
    *,
    logger: logging.Logger | None = None,
) -> MarketProvider:
    cfg = config or load_defaults()
    market_cfg = cfg.get("market", {})
    tz = market_cfg.get("tz", "Asia/Kolkata")
    log = logger or logging.getLogger(__name__)

    api_key = os.getenv("KITE_API_KEY")
    access_token = os.getenv("KITE_ACCESS_TOKEN")

    if not api_key or not access_token or KiteMarketProvider is None:
        log.info("Using MockMarketProvider (missing Kite credentials)")
        return MockMarketProvider(tz=tz)

    try:
        return KiteMarketProvider(config=cfg, logger=log)
    except Exception as exc:  # pragma: no cover - runtime only
        log.error("Falling back to MockMarketProvider: %s", exc)
        return MockMarketProvider(tz=tz)


__all__ = ["resolve_provider", "MarketProvider", "MockMarketProvider"]
