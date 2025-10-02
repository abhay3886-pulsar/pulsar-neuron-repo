from __future__ import annotations

from typing import Any, Callable, Dict


def Field(*, default: Any = None, default_factory: Callable[[], Any] | None = None, **_: Any) -> Any:
    if default_factory is not None:
        return default_factory()
    return default


class BaseModel:
    def __init__(self, **data: Any) -> None:
        annotations = getattr(self.__class__, "__annotations__", {})
        for field in annotations:
            if field in data:
                value = data[field]
            else:
                value = getattr(self.__class__, field, None)
            setattr(self, field, value)

    def model_dump(self) -> Dict[str, Any]:
        result: Dict[str, Any] = {}
        annotations = getattr(self.__class__, "__annotations__", {})
        for field in annotations:
            value = getattr(self, field)
            if isinstance(value, BaseModel):
                result[field] = value.model_dump()
            else:
                result[field] = value
        return result
