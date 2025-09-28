from __future__ import annotations

import os
from typing import Literal

from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings


class BrokerSettings(BaseModel):
    api_key: str = ""
    api_secret: str = ""
    access_token: str = ""


class TelegramSettings(BaseModel):
    bot_token: str = ""
    chat_id: str = ""


class DBSettings(BaseModel):
    dsn: str = ""


class AppSettings(BaseSettings):
    env: Literal["dev", "staging", "prod"] = "dev"
    timezone: str = Field(default=os.getenv("PULSAR_TIMEZONE", "Asia/Kolkata"))
    symbols_index: list[str] = ["NIFTY", "BANKNIFTY"]
    secrets_provider: Literal["env", "aws", "gcp"] = Field(
        default=os.getenv("PULSAR_SECRETS_PROVIDER", "env")
    )

    broker: BrokerSettings = BrokerSettings()
    telegram: TelegramSettings = TelegramSettings()
    db: DBSettings = DBSettings()

    class Config:
        env_prefix = "PULSAR_"
        env_file = os.getenv("ENV_FILE", ".env")


def load_settings() -> AppSettings:
    """Load settings from env/.env and optionally hydrate from secret providers."""
    from .secrets import aws as aws_secrets
    from .secrets import gcp as gcp_secrets

    cfg = AppSettings()
    if cfg.secrets_provider == "aws":
        cfg = aws_secrets.enrich_settings_from_aws(cfg)
    elif cfg.secrets_provider == "gcp":
        cfg = gcp_secrets.enrich_settings_from_gcp(cfg)
    return cfg
