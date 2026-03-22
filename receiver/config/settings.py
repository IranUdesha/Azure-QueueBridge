# ---------------------------------------------------------------------------
# settings.py  (receiver)
#
# Loads all configuration from environment variables (with .env file support).
# Returns a frozen Settings dataclass after validating every value.
# Fails fast at startup if any required variable is missing or out of range.
# ---------------------------------------------------------------------------
from __future__ import annotations

import os                          # Read environment variables
import re                          # Validate queue name format
from dataclasses import dataclass  # Immutable config container
from pathlib import Path           # Resolve .env file path relative to project root
from typing import Optional

from dotenv import load_dotenv     # Load .env file into os.environ


# Azure Storage queue name rules: 3-63 chars, lowercase alphanumeric + hyphens,
# must start/end with a letter or digit, no consecutive hyphens.
_QUEUE_NAME_RE = re.compile(r"^[a-z0-9](?:[a-z0-9-]{1,61}[a-z0-9])?$")


@dataclass(frozen=True)
class Settings:
    """Immutable application configuration. All values are validated at creation time."""

    # --- Azure Storage connection ---
    connection_string: str              # Full Azure Storage connection string
    queue_name: str                     # Name of the Azure Storage Queue to receive messages from

    # --- Message behaviour ---
    message_ttl_seconds: int            # Max time (seconds) a message lives in the queue before Azure auto-deletes it (NOT the invisibility timeout)

    # --- Receiver polling ---
    poll_interval_seconds: float        # Seconds to sleep between polls when no messages are found
    visibility_timeout_seconds: int     # Seconds a message stays invisible to other consumers after being read
    max_messages_per_poll: int          # Number of messages to fetch per receive call (Azure max: 32)
    max_dequeue_count: int              # If a message has been read this many times without being deleted, treat it as poison

    # --- Azure SDK reliability ---
    azure_sdk_timeout_seconds: int      # HTTP request timeout for each Azure SDK call
    azure_sdk_retry_total: int          # Max retry attempts on transient failures
    azure_sdk_retry_backoff_factor: float  # Exponential backoff multiplier between retries
    azure_sdk_retry_backoff_max: int    # Cap on backoff delay (seconds)

    # --- Logging ---
    log_level: str                      # Python log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)


def load_settings(dotenv_path: Optional[str] = None) -> Settings:
    """Load environment variables from .env, validate them, and return a Settings instance."""

    # Default .env location: one directory up from config/ (i.e. the receiver project root)
    if not dotenv_path:
        dotenv_path = str(Path(__file__).resolve().parents[1] / ".env")
    load_dotenv(dotenv_path=dotenv_path)  # Merge .env values into os.environ (does not overwrite existing vars)

    # --- Build connection string ---
    # Option A: use the full connection string directly if provided
    connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    if not connection_string:
        # Option B: assemble it from individual account name + key
        account_name = _get_required("AZURE_STORAGE_ACCOUNT_NAME")
        account_key = _get_required("AZURE_STORAGE_ACCOUNT_KEY")
        endpoint_suffix = os.getenv("AZURE_STORAGE_ENDPOINT_SUFFIX", "core.windows.net")
        connection_string = _build_connection_string(
            account_name=account_name,
            account_key=account_key,
            endpoint_suffix=endpoint_suffix,
        )

    queue_name = _get_required("AZURE_STORAGE_QUEUE_NAME")
    _validate_queue_name(queue_name)  # Enforce Azure naming rules before making any SDK calls

    # Assemble validated Settings from env vars (with defaults and range checks)
    return Settings(
        connection_string=connection_string,
        queue_name=queue_name,
        message_ttl_seconds=_get_int("MESSAGE_TTL_SECONDS", default=3600, min_value=1),             # Default: 1 hour
        poll_interval_seconds=_get_float("POLL_INTERVAL_SECONDS", default=2.0, min_value=0.1),      # Default: 2 seconds
        visibility_timeout_seconds=_get_int("VISIBILITY_TIMEOUT_SECONDS", default=30, min_value=1),  # Default: 30 seconds
        max_messages_per_poll=_get_int("MAX_MESSAGES_PER_POLL", default=1, min_value=1, max_value=32),  # Azure limit: 32
        max_dequeue_count=_get_int("MAX_DEQUEUE_COUNT", default=5, min_value=1),                     # Poison message threshold
        azure_sdk_timeout_seconds=_get_int("AZURE_SDK_TIMEOUT_SECONDS", default=30, min_value=1),
        azure_sdk_retry_total=_get_int("AZURE_SDK_RETRY_TOTAL", default=5, min_value=0, max_value=50),
        azure_sdk_retry_backoff_factor=_get_float("AZURE_SDK_RETRY_BACKOFF_FACTOR", default=0.8, min_value=0.0),
        azure_sdk_retry_backoff_max=_get_int("AZURE_SDK_RETRY_BACKOFF_MAX", default=30, min_value=0),
        log_level=os.getenv("LOG_LEVEL", "INFO"),
    )


def _build_connection_string(*, account_name: str, account_key: str, endpoint_suffix: str) -> str:
    """Assemble a full Azure Storage connection string from individual components."""
    return (
        "DefaultEndpointsProtocol=https;"
        f"AccountName={account_name};"
        f"AccountKey={account_key};"
        f"EndpointSuffix={endpoint_suffix}"
    )


def _get_required(name: str) -> str:
    """Read an env var and raise ValueError if it is missing or blank."""
    value = os.getenv(name)
    if value is None or value.strip() == "":
        raise ValueError(f"Missing required environment variable: {name}")
    return value


def _get_int(
    name: str,
    *,
    default: int,
    min_value: Optional[int] = None,
    max_value: Optional[int] = None,
) -> int:
    """Read an env var as int, falling back to default. Validates against optional min/max bounds."""
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        value = default
    else:
        try:
            value = int(raw)
        except ValueError as exc:
            raise ValueError(f"{name} must be an integer. Got: {raw!r}") from exc

    if min_value is not None and value < min_value:
        raise ValueError(f"{name} must be >= {min_value}. Got: {value}")
    if max_value is not None and value > max_value:
        raise ValueError(f"{name} must be <= {max_value}. Got: {value}")

    return value


def _get_float(
    name: str,
    *,
    default: float,
    min_value: Optional[float] = None,
    max_value: Optional[float] = None,
) -> float:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        value = default
    else:
        try:
            value = float(raw)
        except ValueError as exc:
            raise ValueError(f"{name} must be a number. Got: {raw!r}") from exc

    if min_value is not None and value < min_value:
        raise ValueError(f"{name} must be >= {min_value}. Got: {value}")
    if max_value is not None and value > max_value:
        raise ValueError(f"{name} must be <= {max_value}. Got: {value}")

    return value


def _validate_queue_name(queue_name: str) -> None:
    """Enforce Azure Storage queue naming rules so we fail fast before making SDK calls."""
    if len(queue_name) < 3 or len(queue_name) > 63:
        raise ValueError("AZURE_STORAGE_QUEUE_NAME must be 3-63 characters")
    if not _QUEUE_NAME_RE.match(queue_name):
        raise ValueError(
            "AZURE_STORAGE_QUEUE_NAME must be lowercase letters/numbers and hyphens, "
            "start/end with letter/number"
        )
    if "--" in queue_name:
        raise ValueError("AZURE_STORAGE_QUEUE_NAME cannot contain consecutive hyphens")
