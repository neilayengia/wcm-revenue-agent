"""
Logging Configuration
=====================
Sets up structured logging with console + rotating file output.
"""

import os
import logging
from logging.handlers import RotatingFileHandler

from wcm_agent.config import LOG_DIR


def setup_logging(level=logging.INFO):
    """
    Configure logging for the application.

    - Console handler: INFO level, human-readable format
    - File handler: DEBUG level, rotating (10 MB, 5 backups)

    Call this once at startup before any other imports log messages.
    """
    os.makedirs(LOG_DIR, exist_ok=True)

    root_logger = logging.getLogger()

    # Avoid adding duplicate handlers on repeated calls
    if root_logger.handlers:
        return

    root_logger.setLevel(logging.DEBUG)

    # ── Console Handler ──────────────────────────────────
    console_fmt = logging.Formatter(
        "%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
        datefmt="%H:%M:%S",
    )
    console_handler = logging.StreamHandler()
    console_handler.setLevel(level)
    console_handler.setFormatter(console_fmt)
    root_logger.addHandler(console_handler)

    # ── File Handler (rotating) ──────────────────────────
    file_fmt = logging.Formatter(
        "%(asctime)s  %(levelname)-8s  %(name)s  [%(filename)s:%(lineno)d]  %(message)s"
    )
    log_path = os.path.join(LOG_DIR, "wcm_agent.log")
    file_handler = RotatingFileHandler(
        log_path,
        maxBytes=10 * 1024 * 1024,  # 10 MB
        backupCount=5,
        encoding="utf-8",
    )
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(file_fmt)
    root_logger.addHandler(file_handler)

    logging.getLogger(__name__).info(
        "Logging initialised — file: %s", log_path
    )
