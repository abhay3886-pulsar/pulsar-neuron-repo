from __future__ import annotations
import logging
import requests
from pulsar_neuron.config.secrets import get_telegram_credentials

log = logging.getLogger(__name__)


def send_telegram(text: str) -> bool:
    """
    Send a simple message to Telegram using bot token + chat id from secrets.
    Respects APP_ENV: local/ec2 (secrets helper picks the right fields).
    """
    token, chat_id = get_telegram_credentials()
    if not token or not chat_id:
        log.warning("telegram disabled: missing token/chat_id")
        return False
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    try:
        r = requests.post(url, json={"chat_id": chat_id, "text": text, "parse_mode": "HTML"}, timeout=5)
        ok = (r.status_code == 200)
        if not ok:
            log.error("telegram send failed: %s %s", r.status_code, r.text)
        return ok
    except Exception as e:
        log.error("telegram send exception: %s", e)
        return False
