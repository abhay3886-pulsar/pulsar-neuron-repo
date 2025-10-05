from __future__ import annotations
from pulsar_neuron.config.loader import load_config
from pulsar_neuron.service.context_pack import build_from_db

# Import strategy "run" functions if available
try:
    from pulsar_neuron.strategies.s_orb import run as orb_run  # type: ignore
except Exception:
    orb_run = None
try:
    from pulsar_neuron.strategies.s_vwap_reclaim import run as vwap_run  # type: ignore
except Exception:
    vwap_run = None
try:
    from pulsar_neuron.strategies.s_trend_cont import run as trend_run  # type: ignore
except Exception:
    trend_run = None


def _safe(f, x):
    try:
        return f(x) if f else None
    except Exception as e:
        return f"ERR:{e}"


def main():
    cfg = load_config("markets.yaml")
    tokens = cfg.get("tokens") or {}
    symbols = list(tokens.keys())
    if not symbols:
        raise RuntimeError("No symbols in markets.yaml -> tokens")

    ctx = build_from_db(symbols)
    print("symbol, last_5m_ts, sma20_5m, slope_5m, orb, vwap, trend")
    for s in symbols:
        x = ctx.get(s, {})
        orb = _safe(orb_run, x)
        vwp = _safe(vwap_run, x)
        trn = _safe(trend_run, x)
        print(f"{s}, {x.get('last_5m_ts')}, {x.get('sma20_5m')}, {x.get('slope_5m')}, {orb}, {vwp}, {trn}")


if __name__ == "__main__":
    main()
