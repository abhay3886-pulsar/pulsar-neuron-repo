from __future__ import annotations
import time, sys, signal
from datetime import datetime, timezone, timedelta
from pulsar_neuron.db.postgres import get_conn
from pulsar_neuron.telemetry.alerts import send_telegram


def _one(sql: str):
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql)
        row = cur.fetchone()
        return row.get("max") if isinstance(row, dict) else (row[0] if row else None)


def _age(ts):
    if not ts:
        return None
    if isinstance(ts, str):
        try:
            ts = datetime.fromisoformat(ts.replace("Z", ""))
        except Exception:
            return None
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return datetime.now(timezone.utc) - ts


def _check_once():
    last_5m = _one("select max(ts_ist) as max from ohlcv where tf='5m'")
    last_15m = _one("select max(ts_ist) as max from ohlcv where tf='15m'")
    last_oi = _one("select max(ts_ist) as max from fut_oi")
    last_opt = _one("select max(ts_ist) as max from options_chain")

    a5 = _age(last_5m)
    a15 = _age(last_15m)
    aoi = _age(last_oi)
    aop = _age(last_opt)

    alerts = []
    if a5 and a5 > timedelta(minutes=8):
        alerts.append(f"OHLCV 5m stale ({int(a5.total_seconds()/60)}m)")
    if a15 and a15 > timedelta(minutes=20):
        alerts.append(f"OHLCV 15m stale ({int(a15.total_seconds()/60)}m)")
    if aoi and aoi > timedelta(minutes=15):
        alerts.append(f"Fut OI stale ({int(aoi.total_seconds()/60)}m)")
    if aop and aop > timedelta(minutes=20):
        alerts.append(f"Options stale ({int(aop.total_seconds()/60)}m)")

    return alerts


def main():
    stop = False

    def _sig(*_):
        nonlocal stop
        stop = True

    signal.signal(signal.SIGINT, _sig)
    signal.signal(signal.SIGTERM, _sig)

    while not stop:
        try:
            alerts = _check_once()
            if alerts:
                send_telegram("⚠️ <b>Pulsar Monitor</b>\n" + "\n".join("• " + a for a in alerts))
        except Exception as e:
            print("[monitor] error:", e, file=sys.stderr)
        # check every 60s
        for _ in range(60):
            if stop:
                break
            time.sleep(1)

    print("[monitor] shutdown complete")


if __name__ == "__main__":
    main()
