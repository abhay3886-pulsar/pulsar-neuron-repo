"""Minimal pydantic-compatible stubs for offline testing."""

from __future__ import annotations

import json
from copy import deepcopy
from typing import Any, Dict


class ConfigDict(dict):
    """Placeholder for pydantic.ConfigDict."""

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)


class _Sentinel:  # pragma: no cover - internal helper
    pass


_MISSING = _Sentinel()


class _FieldInfo:
    def __init__(self, *, default: Any = _MISSING, default_factory: Any = _MISSING) -> None:
        self.default = default
        self.default_factory = default_factory


def Field(*, default: Any = _MISSING, default_factory: Any = _MISSING) -> _FieldInfo:
    """Return placeholder field metadata."""

    return _FieldInfo(default=default, default_factory=default_factory)


class BaseModel:
    """Very small subset of Pydantic's BaseModel API."""

    model_config: ConfigDict = ConfigDict()

    def __init__(self, **data: Any) -> None:
        values: Dict[str, Any] = {}
        annotations = getattr(self, "__annotations__", {})
        for name in annotations:
            if name in data:
                values[name] = data.pop(name)
                continue
            field_def = getattr(self.__class__, name, _MISSING)
            if isinstance(field_def, _FieldInfo):
                if field_def.default_factory is not _MISSING:
                    values[name] = field_def.default_factory()
                elif field_def.default is not _MISSING:
                    values[name] = deepcopy(field_def.default)
                else:
                    raise TypeError(f"Missing required field: {name}")
            elif field_def is not _MISSING:
                values[name] = deepcopy(field_def)
            else:
                raise TypeError(f"Missing required field: {name}")
        extra_policy = self.model_config.get("extra") if isinstance(self.model_config, dict) else None
        if data and extra_policy == "forbid":
            unknown = ", ".join(sorted(data))
            raise TypeError(f"Extra fields not permitted: {unknown}")
        for key, value in values.items():
            setattr(self, key, value)

    def _coerce(self, value: Any) -> Any:
        if isinstance(value, BaseModel):
            return value.model_dump()
        if isinstance(value, list):
            return [self._coerce(item) for item in value]
        if isinstance(value, dict):
            return {key: self._coerce(val) for key, val in value.items()}
        return value

    def model_dump(self) -> Dict[str, Any]:
        return {
            name: self._coerce(getattr(self, name))
            for name in getattr(self, "__annotations__", {})
        }

    def model_dump_json(self) -> str:
        return json.dumps(self.model_dump())

    def model_copy(self, *, update: Dict[str, Any] | None = None, deep: bool | None = None) -> "BaseModel":
        data = self.model_dump()
        if update:
            data.update(update)
        return self.__class__(**data)

    def __repr__(self) -> str:  # pragma: no cover - debug helper
        return f"{self.__class__.__name__}({self.model_dump()!r})"
