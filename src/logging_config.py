"""Centralised logging configuration for the IBKR Trade Journal."""

import logging
import sys


def setup_logging(level: int = logging.INFO) -> None:
    """Configure root logger with a consistent format.

    Call once at app startup (app.py). All modules that use
    ``logging.getLogger(__name__)`` will inherit this config.
    """
    root = logging.getLogger()
    if root.handlers:
        return  # already configured (e.g. Streamlit hot-reload)

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(
        logging.Formatter(
            "%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
    )
    root.setLevel(level)
    root.addHandler(handler)
