from __future__ import annotations

import json
import os
from typing import Any, Dict


def _client():
    try:
        from google.cloud import secretmanager

        return secretmanager.SecretManagerServiceClient()
    except Exception as e:  # pragma: no cover - optional dep
        print(f"[GCP-SECRETS] Import error: {e}")
        return None


def get_secret_string(project_id: str, secret_name: str, version: str = "latest") -> str | None:
    client = _client()
    if not client:
        return None
    try:
        name = f"projects/{project_id}/secrets/{secret_name}/versions/{version}"
        resp = client.access_secret_version(name=name)
        return resp.payload.data.decode("UTF-8")
    except Exception as e:  # pragma: no cover - network stub
        print(f"[GCP-SECRETS] Error: {e}")
        return None


def enrich_settings_from_gcp(cfg):
    """Minimal stub: expects a JSON string with compatible keys."""
    project = os.getenv("GCP_PROJECT_ID")
    name = os.getenv("GCP_SECRET_NAME")
    if project and name:
        data = get_secret_string(project, name)
        if data:
            try:
                blob: Dict[str, Any] = json.loads(data)
                cfg.db.dsn = cfg.db.dsn or blob.get("DB_DSN", "")
                cfg.telegram.bot_token = cfg.telegram.bot_token or blob.get("TELEGRAM_BOT_TOKEN", "")
                cfg.telegram.chat_id = cfg.telegram.chat_id or blob.get("TELEGRAM_CHAT_ID", "")
                cfg.broker.api_key = cfg.broker.api_key or blob.get("KITE_API_KEY", "")
                cfg.broker.api_secret = cfg.broker.api_secret or blob.get("KITE_API_SECRET", "")
                cfg.broker.access_token = cfg.broker.access_token or blob.get("KITE_ACCESS_TOKEN", "")
            except Exception as e:  # pragma: no cover - parsing stub
                print(f"[GCP-SECRETS] JSON parse error: {e}")
    return cfg
