"""Logging helpers."""

from __future__ import annotations

import logging
import sys
from pathlib import Path
from typing import Optional, Union

try:
    from loguru import logger as _loguru_logger
except ImportError:  # pragma: no cover - fallback for minimal environments
    _loguru_logger = None


def setup_logger(level: str = "INFO", log_file: Optional[Union[str, Path]] = None):
    """Configure the shared logger."""
    if _loguru_logger is not None:
        _loguru_logger.remove()
        _loguru_logger.add(sys.stderr, level=level.upper(), colorize=False)
        if log_file is not None:
            _loguru_logger.add(str(log_file), level=level.upper(), rotation="1 week")
        return _loguru_logger

    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        stream=sys.stderr,
    )
    fallback_logger = logging.getLogger("investment_agent")
    if log_file is not None:
        file_handler = logging.FileHandler(str(log_file))
        file_handler.setFormatter(
            logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s")
        )
        fallback_logger.addHandler(file_handler)
    return fallback_logger


logger = setup_logger()
