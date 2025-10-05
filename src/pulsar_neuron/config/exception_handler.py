"""Shared exception handling utilities."""

from __future__ import annotations

import logging
import sys
import traceback


def setup_exception_hook() -> None:
    """Install a logging-based exception hook once per process."""

    if getattr(setup_exception_hook, "_installed", False):  # type: ignore[attr-defined]
        return

    logger = logging.getLogger(__name__)

    def _log_exception(exc_type, exc, exc_tb):
        formatted = "".join(traceback.format_exception(exc_type, exc, exc_tb))
        logger.error("Unhandled exception: %s", formatted)

    sys.excepthook = _log_exception
    setattr(setup_exception_hook, "_installed", True)  # type: ignore[attr-defined]
