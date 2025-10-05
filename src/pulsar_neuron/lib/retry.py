from __future__ import annotations
import time
from typing import Callable, Type, Tuple


def retry(
    tries: int = 3,
    delay: float = 0.5,
    backoff: float = 2.0,
    exc: Tuple[Type[BaseException], ...] = (Exception,),
):
    def deco(fn: Callable):
        def wrapped(*args, **kwargs):
            _tries, _delay = tries, delay
            while _tries > 1:
                try:
                    return fn(*args, **kwargs)
                except exc:
                    time.sleep(_delay)
                    _tries -= 1
                    _delay *= backoff
            return fn(*args, **kwargs)

        return wrapped

    return deco
