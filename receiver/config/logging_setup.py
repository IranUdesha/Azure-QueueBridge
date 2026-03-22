# ---------------------------------------------------------------------------
# logging_setup.py  (receiver)
#
# Configures Python's built-in logging with a consistent timestamp format.
# Called once at startup; all modules then use logging.getLogger(__name__).
# ---------------------------------------------------------------------------
import logging


def configure_logging(level: str) -> None:
    """Set up root logger with the given level and a timestamped format."""
    logging.basicConfig(
        level=(level or "INFO").upper(),                           # Fall back to INFO if level is empty/None
        format="%(asctime)s %(levelname)s %(name)s %(message)s",   # Example: 2026-03-21 12:00:00,000 INFO receiver_worker Message received
    )
