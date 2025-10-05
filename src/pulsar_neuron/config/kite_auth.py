from __future__ import annotations

import json
import logging
import os
import threading
import time
from typing import Dict

from pulsar_neuron.config.secrets import get_secret_json

log = logging.getLogger(__name__)

# Default token file for local dev (updated by your Pulsar Algo token service)
LOCAL_TOKEN_FILE = os.path.expanduser("~/.pulsar/kite_tokens.json")

# AWS secret for EC2 mode
SECRET_ID = "pulsar-neuron/kite-tokens"


def _load_from_file() -> dict:
    """Load kite tokens from ~/.pulsar/kite_tokens.json."""
    if not os.path.exists(LOCAL_TOKEN_FILE):
        raise FileNotFoundError(f"Token file not found: {LOCAL_TOKEN_FILE}")
    with open(LOCAL_TOKEN_FILE, "r") as f:
        data = json.load(f)
    if "api_key" not in data or "access_token" not in data:
        raise KeyError("kite_tokens.json missing api_key or access_token")
    return data


def load_kite_creds() -> dict:
    """
    Load Kite credentials:
    - If AWS Secrets Manager key exists (in EC2 env), use it.
    - Otherwise, fall back to local ~/.pulsar/kite_tokens.json.
    """
    env = os.getenv("APP_ENV", "local")
    if env == "ec2":
        try:
            secret = get_secret_json(SECRET_ID)
            log.info("✅ Loaded Kite creds from AWS secret %s", SECRET_ID)
            return secret
        except Exception as e:
            log.warning("⚠️ Failed to fetch secret %s, fallback to file: %s", SECRET_ID, e)

    return _load_from_file()


class TokenWatcher:
    """
    Watches kite_tokens.json for updates and notifies live_bars.
    """

    def __init__(self, poll_interval: float = 30.0):
        self._path = LOCAL_TOKEN_FILE
        self._poll_interval = poll_interval
        self._last_mtime = 0.0
        self._changed = threading.Event()
        self._thread = threading.Thread(target=self._poll, daemon=True)
        self._thread.start()

    def _poll(self):
        """Poll file modification time."""
        while True:
            try:
                mtime = os.path.getmtime(self._path)
                if mtime != self._last_mtime:
                    self._last_mtime = mtime
                    self._changed.set()
            except FileNotFoundError:
                pass
            time.sleep(self._poll_interval)

    def wait_for_change(self, timeout: float | None = None) -> bool:
        """Block until file changes (or timeout)."""
        changed = self._changed.wait(timeout)
        if changed:
            self._changed.clear()
        return changed
