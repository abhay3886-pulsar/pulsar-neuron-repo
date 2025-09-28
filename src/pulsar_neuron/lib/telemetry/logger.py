from __future__ import annotations

from loguru import logger


def setup_logger():
    logger.remove()
    logger.add(lambda msg: print(msg, end=""), level="INFO")
    return logger
