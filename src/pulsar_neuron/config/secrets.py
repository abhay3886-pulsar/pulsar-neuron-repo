"""Helpers for fetching secrets from AWS Secrets Manager.

This module provides a unified interface for retrieving JSON secrets with a
simple in-memory cache. All Pulsar Neuron components should use these helpers
instead of duplicating AWS access logic across the codebase.
"""

from __future__ import annotations

import json
import logging
import os
import time
from typing import Dict

try:
    import boto3
except ImportError:  # pragma: no cover - optional dependency
    boto3 = None  # type: ignore[assignment]


from .exception_handler import setup_exception_hook

logger = logging.getLogger(__name__)
setup_exception_hook()

# secret_id -> (value, fetched_at)
_CACHE: Dict[str, tuple[dict, float]] = {}


def get_secret_json(secret_id: str, *, ttl_seconds: int = 900) -> dict:
    """Return secret as JSON dict, cached for ``ttl_seconds``."""

    now = time.time()
    cached = _CACHE.get(secret_id)
    if cached and now - cached[1] < ttl_seconds:
        return cached[0]

    if boto3 is None:
        raise ImportError("boto3 is required to fetch AWS secrets")

    region = os.getenv("AWS_REGION", "ap-south-1")
    client = boto3.client("secretsmanager", region_name=region)
    try:
        resp = client.get_secret_value(SecretId=secret_id)
        value = json.loads(resp["SecretString"])
        _CACHE[secret_id] = (value, now)
        return value
    except Exception as e:  # pragma: no cover - network call
        logger.error("❌ Failed to fetch secret %s: %s", secret_id, e)
        raise RuntimeError(f"Unable to fetch secret {secret_id}") from e


def refresh_now(secret_id: str) -> dict:
    """Force refresh a secret from AWS Secrets Manager."""

    _CACHE.pop(secret_id, None)
    return get_secret_json(secret_id)


# ───── Specific Pulsar Neuron accessors ─────


def get_db_credentials() -> dict:
    """Return Pulsar Neuron DB credentials."""

    return get_secret_json("pulsar/db")


def get_kite_credentials() -> dict:
    """Return Kite credentials (api_key, access_token)."""

    return get_secret_json("pulsar/kite")


def get_services_credentials() -> dict:
    """Return other service credentials (telegram, openai, etc.)."""

    return get_secret_json("pulsar/services")


def get_telegram_credentials(env_var: str = "APP_ENV") -> tuple[str | None, str | None]:
    """Return Telegram bot token and chat id based on environment."""

    services_secret = get_services_credentials()
    env = os.getenv(env_var, "local")
    if env == "ec2":
        token = services_secret.get("telegram_bot_token_ec2")
        chat_id = services_secret.get("telegram_chat_id_ec2")
    else:
        token = services_secret.get("telegram_bot_token_local")
        chat_id = services_secret.get("telegram_chat_id_local")
    return token, chat_id
