# ---------------------------------------------------------------------------
# logging_setup.py  (sender)
#
# Configures Python's built-in logging with a consistent timestamp format.
# Called once at startup; all modules then use logging.getLogger(__name__).
# ---------------------------------------------------------------------------
import logging
from pathlib import Path
from typing import Optional


def configure_logging(level: str, log_file_path: Optional[str] = None, log_file_name: Optional[str] = None) -> None:
    """Set up root logger with the given level and a timestamped format."""
    resolved_level = getattr(logging, (level or "INFO").upper(), logging.INFO)

    handlers: list[logging.Handler] = [logging.StreamHandler()]
    if log_file_path and log_file_name:
        file_path = Path(log_file_path)
        file_path.mkdir(parents=True, exist_ok=True)
        handlers.append(logging.FileHandler(file_path / log_file_name, encoding="utf-8"))

    logging.basicConfig(
        level=resolved_level,                                       # Fall back to INFO if level is empty/None
        format="%(asctime)s %(levelname)s %(name)s %(message)s",   # Example: 2026-03-21 12:00:00,000 INFO sender_api Message sent
        handlers=handlers,
        force=True,
    )

    # Keep noisy transport logs hidden at INFO and below; only show them in DEBUG sessions.
    transport_level = logging.DEBUG if resolved_level <= logging.DEBUG else logging.WARNING
    logging.getLogger("urllib3.connectionpool").setLevel(transport_level)
    logging.getLogger("azure.core.pipeline.policies.http_logging_policy").setLevel(transport_level)
