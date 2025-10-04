    """OHLCV repo
    NOTE: Stub module. Add real logic later.
    """

def upsert_bars(rows: list[dict]): ...
def latest_bar(symbol: str, tf: str) -> dict: ...


def upsert_many(rows: list[dict]) -> int:
    """Compat helper mirroring the other repo interfaces."""

    if not rows:
        return 0

    result = upsert_bars(rows)
    if isinstance(result, int):
        return result

    return len(rows)

