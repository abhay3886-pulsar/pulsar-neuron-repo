from __future__ import annotations

import json
import os
from typing import Any, Dict


def _boto_session():
    import boto3

    region = os.getenv("AWS_REGION", "ap-south-1")
    return boto3.session.Session(region_name=region)


def get_secret_json(secret_name: str) -> Dict[str, Any] | None:
    """
    Fetch a JSON secret from AWS Secrets Manager.
    Returns dict or None if not found / error.
    """
    try:
        session = _boto_session()
        client = session.client("secretsmanager")
        resp = client.get_secret_value(SecretId=secret_name)
        secret_str = resp.get("SecretString")
        if secret_str:
            return json.loads(secret_str)
    except Exception as e:  # pragma: no cover - network stub
        print(f"[AWS-SECRETS] Error: {e}")
    return None


def get_param(name: str, with_decryption: bool = True) -> str | None:
    """
    Fetch a parameter from AWS SSM Parameter Store (optionally decrypted).
    """
    try:
        session = _boto_session()
        ssm = session.client("ssm")
        resp = ssm.get_parameter(Name=name, WithDecryption=with_decryption)
        return resp["Parameter"]["Value"]
    except Exception as e:  # pragma: no cover - network stub
        print(f"[AWS-SSM] Error: {e}")
    return None


def enrich_settings_from_aws(cfg):
    """
    Merge settings from AWS Secrets/SSM into pydantic settings object.
    Priority: existing cfg values > Secrets Manager JSON > SSM params > env.
    Expected JSON structure in Secrets Manager (example):
    {
      "DB_DSN": "...",
      "TELEGRAM_BOT_TOKEN": "...",
      "TELEGRAM_CHAT_ID": "...",
      "KITE_API_KEY": "...",
      "KITE_API_SECRET": "...",
      "KITE_ACCESS_TOKEN": "..."
    }
    """
    secret_name = os.getenv("AWS_SECRETSMGR_SECRET_NAME")
    if secret_name:
        blob = get_secret_json(secret_name) or {}
        cfg.db.dsn = cfg.db.dsn or blob.get("DB_DSN", "")
        cfg.telegram.bot_token = cfg.telegram.bot_token or blob.get("TELEGRAM_BOT_TOKEN", "")
        cfg.telegram.chat_id = cfg.telegram.chat_id or blob.get("TELEGRAM_CHAT_ID", "")
        cfg.broker.api_key = cfg.broker.api_key or blob.get("KITE_API_KEY", "")
        cfg.broker.api_secret = cfg.broker.api_secret or blob.get("KITE_API_SECRET", "")
        cfg.broker.access_token = cfg.broker.access_token or blob.get("KITE_ACCESS_TOKEN", "")

    prefix = os.getenv("AWS_SSM_PARAM_PREFIX")
    if prefix:
        cfg.db.dsn = cfg.db.dsn or get_param(prefix + "DB_DSN") or ""
        cfg.telegram.bot_token = cfg.telegram.bot_token or get_param(prefix + "TELEGRAM_BOT_TOKEN") or ""
        cfg.telegram.chat_id = cfg.telegram.chat_id or get_param(prefix + "TELEGRAM_CHAT_ID") or ""
        cfg.broker.api_key = cfg.broker.api_key or get_param(prefix + "KITE_API_KEY") or ""
        cfg.broker.api_secret = cfg.broker.api_secret or get_param(prefix + "KITE_API_SECRET") or ""
        cfg.broker.access_token = cfg.broker.access_token or get_param(prefix + "KITE_ACCESS_TOKEN") or ""
    return cfg
