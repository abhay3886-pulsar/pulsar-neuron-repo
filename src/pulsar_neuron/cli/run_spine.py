"""Offline spine runner for Pulsar Neuron."""
from __future__ import annotations

import logging
from typing import Any, Dict

from pulsar_neuron.ingest import ohlcv_job
from pulsar_neuron.service import context_pack
from pulsar_neuron.strategies import s_orb, s_vwap_reclaim

LOGGER = logging.getLogger(__name__)


def _configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )


def _summarize_context(ctx: Any) -> str:
    if isinstance(ctx, dict):
        parts = []
        for key, value in ctx.items():
            if isinstance(value, (int, float, str, bool)):
                parts.append(f"{key}={value}")
            elif value is None:
                parts.append(f"{key}=None")
            else:
                parts.append(f"{key}=<{type(value).__name__}>")
        return ", ".join(parts) if parts else "<empty>"
    return str(ctx)


def _build_context(symbol: str, bars: Any) -> Any:
    builder = getattr(context_pack, "build", None)
    if callable(builder):
        return builder(bars)

    LOGGER.warning("context_pack.build missing, falling back to build_context stub")
    fallback = getattr(context_pack, "build_context", None)
    if callable(fallback):
        return fallback(symbol=symbol, now_ist="mock")

    raise AttributeError("context_pack does not provide build or build_context")


def _evaluate_strategy(module: Any, ctx: Any, name: str) -> Any:
    evaluate = getattr(module, "evaluate", None)
    if callable(evaluate):
        try:
            return evaluate(ctx)
        except Exception:  # pragma: no cover - defensive logging
            LOGGER.exception("Strategy %s.evaluate raised an exception", name)
            return "error"

    checklist = getattr(module, "checklist", None)
    if callable(checklist):
        try:
            return checklist(ctx)
        except Exception:  # pragma: no cover - defensive logging
            LOGGER.exception("Strategy %s.checklist raised an exception", name)
            return "error"

    LOGGER.warning("Strategy module %s has no evaluate/checklist entry point", name)
    return "unavailable"


def main() -> None:
    _configure_logging()
    print("🚀 Pulsar Neuron — Offline Spine (Mock Mode)")

    try:
        bars: Dict[str, Any] = ohlcv_job.run(
            symbols=["NIFTY 50", "NIFTY BANK"],
            tf="5m",
            mode="mock",
        )
        LOGGER.info("Fetched OHLCV bars for %d symbols", len(bars))
    except Exception as exc:  # pragma: no cover - runtime guard
        LOGGER.exception("Failed to run OHLCV job: %s", exc)
        return

    if not bars:
        LOGGER.warning("No bars returned from OHLCV job")
        return

    for symbol, symbol_bars in bars.items():
        try:
            ctx = _build_context(symbol, symbol_bars)
            summary = _summarize_context(ctx)
            orb_state = _evaluate_strategy(s_orb, ctx, "s_orb")
            vwap_state = _evaluate_strategy(s_vwap_reclaim, ctx, "s_vwap_reclaim")
        except Exception as exc:  # pragma: no cover - runtime guard
            LOGGER.exception("Failed processing symbol %s: %s", symbol, exc)
            continue

        print(f"\n===== {symbol} =====")
        print(f"Context: {summary}")
        print(f"ORB: {orb_state}")
        print(f"VWAP Reclaim: {vwap_state}")
        print("--------------------------")


if __name__ == "__main__":
    main()
