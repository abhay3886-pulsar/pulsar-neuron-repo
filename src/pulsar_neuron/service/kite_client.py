from __future__ import annotations
import logging
from typing import Dict, List, Any
from pulsar_neuron.config.kite_auth import load_kite_creds
from pulsar_neuron.lib.retry import retry

log = logging.getLogger(__name__)

try:
    from kiteconnect import KiteConnect  # type: ignore
except Exception:  # pragma: no cover
    KiteConnect = None  # type: ignore


class KiteRest:
    def __init__(self):
        if KiteConnect is None:
            raise RuntimeError("kiteconnect is not installed. pip install kiteconnect")
        creds = load_kite_creds()
        self.api = KiteConnect(api_key=creds["api_key"])
        self.api.set_access_token(creds["access_token"])

    @retry(tries=3, delay=0.4, backoff=2.0)
    def quote(self, tokens: List[int]) -> Dict[str, Any]:
        """Fetch quote for multiple instrument tokens."""
        # Kite expects list of "exchange:tradingsymbol" or instrument tokens.
        # We pass raw tokens; the client handles it.
        return self.api.quote(tokens)  # type: ignore
