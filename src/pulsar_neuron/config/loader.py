from __future__ import annotations

from copy import deepcopy
from functools import lru_cache
from pathlib import Path
import os
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


def _coerce_env_value(value: str) -> Any:
    lowered = value.lower()
    if lowered in {"true", "false"}:
        return lowered == "true"
    try:
        if "." in value:
            return float(value)
        return int(value)
    except ValueError:
        pass
    if value.startswith("[") and value.endswith("]"):
        try:
            import json

            return json.loads(value)
        except Exception:  # pragma: no cover - lenient parsing
            return value
    return value


def _apply_env_overrides(config: dict[str, Any]) -> dict[str, Any]:
    result = deepcopy(config)

    def _resolve_key(mapping: dict[str, Any], key: str) -> str:
        for existing in mapping:
            if existing.lower() == key:
                return existing
        return key

    for env_key, env_value in os.environ.items():
        if "__" not in env_key:
            continue
        parts = [p.lower() for p in env_key.split("__") if p]
        if not parts:
            continue
        top = parts[0]
        matched_top = None
        for existing in result:
            if existing.lower() == top:
                matched_top = existing
                break
        if matched_top is None:
            continue
        cursor: Any = result
        for segment in parts[:-1]:
            if not isinstance(cursor, dict):
                cursor = None
                break
            actual = _resolve_key(cursor, segment)
            if actual not in cursor or not isinstance(cursor[actual], dict):
                cursor[actual] = {}
            cursor = cursor[actual]
        if not isinstance(cursor, dict):
            continue
        final_key = parts[-1]
        actual_final = _resolve_key(cursor, final_key)
        cursor[actual_final] = _coerce_env_value(env_value)
    return result


@lru_cache(maxsize=None)
def load_config(name: str) -> dict[str, Any]:
    """Load a YAML config by filename relative to the ``config`` directory."""

    path = _CONFIG_ROOT / name
    data = _load_yaml(path)
    return _apply_env_overrides(data)


def load_defaults() -> dict[str, Any]:
    return load_config("defaults.yaml")


def load_markets() -> dict[str, Any]:
    return load_config("markets.yaml")


def load_prompts() -> dict[str, Any]:
    return load_config("prompts.yaml")
