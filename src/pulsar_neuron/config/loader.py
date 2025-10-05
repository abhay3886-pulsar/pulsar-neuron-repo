"""Utility helpers to load YAML configuration files."""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Any

import yaml


_CONFIG_ROOT = Path(__file__).resolve().parents[3] / "config"


def _load_yaml(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {path}")
    with path.open("r", encoding="utf-8") as fh:
        data = yaml.safe_load(fh) or {}
    if not isinstance(data, dict):
        raise ValueError(f"Expected mapping in config file {path}")
    return data


@lru_cache(maxsize=None)
def load_config(name: str) -> dict[str, Any]:
    """Load a YAML config by filename relative to the ``config`` directory."""

    path = _CONFIG_ROOT / name
    return _load_yaml(path)


def load_defaults() -> dict[str, Any]:
    return load_config("defaults.yaml")


def load_markets() -> dict[str, Any]:
    return load_config("markets.yaml")


def load_prompts() -> dict[str, Any]:
    return load_config("prompts.yaml")
