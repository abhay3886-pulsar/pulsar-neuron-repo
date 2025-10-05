from __future__ import annotations
from pulsar_neuron.config.loader import load_config
from pulsar_neuron.service.context_pack import build_from_db
from pulsar_neuron.telemetry.alerts import send_telegram

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


def _fmt_line(s, x, orb, vwp, trn):
    return (
        f"<b>{s}</b>  ⏱ {x.get('last_5m_ts')}\n"
        f"• sma20_5m: {x.get('sma20_5m')}  slope_5m: {x.get('slope_5m')}\n"
        f"• ORB: {orb}  VWAP: {vwp}  TREND: {trn}"
    )


def main():
    cfg = load_config("markets.yaml")
    tokens = cfg.get("tokens") or {}
    symbols = list(tokens.keys())
    if not symbols:
        raise RuntimeError("No symbols in markets.yaml -> tokens")

    ctx = build_from_db(symbols)
    lines = []
    for s in symbols:
        x = ctx.get(s, {})
        orb = _safe(orb_run, x)
        vwp = _safe(vwap_run, x)
        trn = _safe(trend_run, x)
        lines.append(_fmt_line(s, x, orb, vwp, trn))

    text = "📈 <b>Pulsar-Neuron Signals</b>\n" + "\n\n".join(lines)
    print(text)  # stdout for logs
    send_telegram(text)


if __name__ == "__main__":
    main()
