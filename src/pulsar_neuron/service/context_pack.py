    """Context pack builder
    NOTE: Stub module. Add real logic later.
    """

def build_context(symbol: str, now_ist: str) -> dict:
    """Return compact facts + freshness + ok."""
    return {
        "symbol": symbol,
        "now": now_ist,
        "ok": False,
        "payload": {}
    }

