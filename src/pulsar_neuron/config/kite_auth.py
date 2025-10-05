from __future__ import annotations

import json
import os
import time
from pathlib import Path
from typing import Dict

from pulsar_neuron.config.secrets import get_kite_credentials

DEFAULT_TOKEN_FILE = Path(os.getenv("KITE_TOKEN_FILE", "~/.config/pulsar/kite_token.json")).expanduser()


def _load_from_file(p: Path) -> Dict[str, str] | None:
    if not p.exists():
        return None
    try:
        data = json.loads(p.read_text())
        ak, at = data.get("api_key"), data.get("access_token")
        if ak and at:
            return {"api_key": ak, "access_token": at}
    except Exception:
        pass
    return None


def load_kite_creds() -> Dict[str, str]:
    """Prefer local token file (written by Pulsar-Algo), else fallback to AWS Secrets Manager."""

    creds = _load_from_file(DEFAULT_TOKEN_FILE)
    return creds or get_kite_credentials()


class TokenWatcher:
    """Watches token file for modification and triggers reconnect when changed."""

    def __init__(self, path: Path = DEFAULT_TOKEN_FILE, poll_secs: float = 10.0):
        self.path = path
        self.poll_secs = poll_secs
        self._last = self._mtime()

    def _mtime(self) -> float | None:
        try:
            return self.path.stat().st_mtime
        except FileNotFoundError:
            return None

    def wait_for_change(self) -> bool:
        while True:
            time.sleep(self.poll_secs)
            try:
                m = self.path.stat().st_mtime
            except FileNotFoundError:
                m = None
            if m != self._last:
                self._last = m
                return True
