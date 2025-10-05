from __future__ import annotations

from copy import deepcopy
from functools import lru_cache
from pathlib import Path
from typing import Any

try:  # pragma: no cover - optional dependency
    import yaml
except ImportError:  # pragma: no cover - optional dependency
    yaml = None  # type: ignore[assignment]


_FALLBACK_CONFIGS: dict[str, dict[str, Any]] = {
    "markets.yaml": {
        "tokens": {
            "NIFTY 50": 256265,
            "NIFTY BANK": 260105,
        },
        "symbols": [
            "NIFTY",
            "BANKNIFTY",
        ],
        "expiries": {
            "default_roll": "weekly",
        },
        "sessions": {
            "india": {
                "tz": "Asia/Kolkata",
                "regular": {
                    "open": "09:15",
                    "close": "15:30",
                },
                "ib_window": {
                    "start": "09:15",
                    "end": "10:15",
                },
            },
        },
    },
}


_CONFIG_ROOT = Path(__file__).resolve().parents[3] / "config"


def _load_yaml(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {path}")

    if yaml is None:
        fallback = _FALLBACK_CONFIGS.get(path.name)
        if fallback is None:
            raise ImportError("PyYAML is required to load configuration files")
        return deepcopy(fallback)

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
