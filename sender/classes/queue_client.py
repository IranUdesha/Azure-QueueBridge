# ---------------------------------------------------------------------------
# queue_client.py
#
# Wrapper around the Azure Storage Queue SDK.
# Provides methods to send JSON messages, receive messages, and delete them.
# Includes a monkey-patch for an Azure SDK bug (see below).
# ---------------------------------------------------------------------------
from __future__ import annotations

import json                                          # Serialize/deserialize message payloads to JSON
import logging                                       # Structured log output
import time                                          # Used for retry delays when queue is being deleted
from dataclasses import dataclass                    # Immutable data container for received messages
from typing import Any, Dict, Iterable, Optional

from azure.core.exceptions import AzureError, ResourceExistsError  # Azure SDK exception types
from azure.core.pipeline.policies import RetryPolicy               # Configures automatic retries on transient failures
from azure.storage.queue import QueueClient, QueueServiceClient    # Azure Queue Storage SDK clients

from config.settings import Settings                 # Validated configuration loaded from .env

logger = logging.getLogger(__name__)                 # Module-scoped logger

# Maximum number of retries when Azure returns QueueBeingDeleted (409).
# With exponential backoff starting at 2s (2+4+8+16+32 = 62s total wait),
# this covers the ~30-second Azure purge window with margin.
_QUEUE_BEING_DELETED_MAX_RETRIES = 5
_QUEUE_BEING_DELETED_BASE_DELAY = 2  # seconds

# ---------------------------------------------------------------------------
# Workaround for Azure SDK bug:
# azure-storage-queue adds 'hosts' and 'location_mode' to the pipeline
# context options (via StorageHosts policy), but azure-core does not strip
# them before passing **kwargs to requests.Session.request(), which rejects
# unknown keyword arguments.
# We patch cleanup_kwargs_for_transport to also remove these storage-specific keys.
# ---------------------------------------------------------------------------
import azure.core.pipeline._base as _pipeline_base  # noqa: E402

_original_cleanup = _pipeline_base.cleanup_kwargs_for_transport  # Save a reference to the original function

def _patched_cleanup(kwargs: Dict[str, str]) -> None:
    """Extended cleanup that also strips storage-specific keys leaked by StorageHosts policy."""
    _original_cleanup(kwargs)              # Run the SDK's original cleanup first
    if kwargs:
        for key in ("hosts", "location_mode"):  # These two keys cause TypeError in requests.Session.request()
            kwargs.pop(key, None)

# Replace the SDK's cleanup function with our patched version at module import time
_pipeline_base.cleanup_kwargs_for_transport = _patched_cleanup


@dataclass(frozen=True)
class ReceivedMessage:
    """Immutable container for a message pulled from the queue."""
    message_id: str                        # Azure-assigned unique ID for this message
    dequeue_count: int                     # How many times this message has been read (useful for poison-message detection)
    raw_text: str                          # The original string content stored in the queue
    json_body: Optional[Dict[str, Any]]    # Parsed JSON payload, or None if parsing failed


class AzureQueue:
    """High-level wrapper around the Azure Queue Storage SDK."""

    def __init__(self, settings: Settings):
        self._settings = settings              # Keep a reference to settings for use in all methods

        # Configure automatic retry with exponential backoff for transient Azure errors
        retry_policy = RetryPolicy(
            total_retries=settings.azure_sdk_retry_total,            # Max number of retry attempts
            retry_backoff_factor=settings.azure_sdk_retry_backoff_factor,  # Multiplier between retries (seconds)
            retry_backoff_max=settings.azure_sdk_retry_backoff_max,  # Cap on backoff delay (seconds)
        )

        # Create a service-level client authenticated with the connection string
        self._service_client = QueueServiceClient.from_connection_string(
            settings.connection_string,
            retry_policy=retry_policy,
        )

        # Get a queue-specific client for the configured queue name
        self._queue_client: QueueClient = self._service_client.get_queue_client(settings.queue_name)

    def ensure_queue_exists(self) -> None:
        """Create the queue in Azure if it doesn't already exist (idempotent).

        Handles the ``QueueBeingDeleted`` race condition: when a queue has been
        recently deleted, Azure returns HTTP 409 for up to ~30 seconds while it
        finishes purging.  This method retries with exponential backoff until
        the queue can be recreated or the maximum number of retries is exhausted.
        """
        for attempt in range(_QUEUE_BEING_DELETED_MAX_RETRIES + 1):
            try:
                self._queue_client.create_queue(timeout=self._settings.azure_sdk_timeout_seconds)
                logger.info("Queue created", extra={"queue": self._settings.queue_name})
                return
            except ResourceExistsError as exc:
                error_code = getattr(exc, "error_code", None) or ""

                if error_code == "QueueBeingDeleted":
                    delay = _QUEUE_BEING_DELETED_BASE_DELAY * (2 ** attempt)
                    logger.warning(
                        "Queue is being deleted by Azure, retrying in %ds (attempt %d/%d)",
                        delay, attempt + 1, _QUEUE_BEING_DELETED_MAX_RETRIES,
                        extra={"queue": self._settings.queue_name},
                    )
                    if attempt < _QUEUE_BEING_DELETED_MAX_RETRIES:
                        time.sleep(delay)
                        continue
                    # Exhausted all retries – re-raise so the caller knows
                    raise RuntimeError(
                        f"Queue '{self._settings.queue_name}' is still being deleted "
                        f"after {_QUEUE_BEING_DELETED_MAX_RETRIES} retries. "
                        "Please wait and try again."
                    ) from exc

                # Any other 409 means the queue already exists – that's fine
                logger.debug("Queue already exists", extra={"queue": self._settings.queue_name})
                return

    def send_json(self, payload: Dict[str, Any]) -> str:
        """Serialize a dict to JSON and send it to the queue. Returns the message ID."""
        self.ensure_queue_exists()              # Make sure the queue exists before sending

        # Compact JSON – no extra whitespace, non-ASCII characters preserved
        message_text = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))

        result = self._queue_client.send_message(
            message_text,
            time_to_live=self._settings.message_ttl_seconds,  # int seconds – Azure auto-deletes the message after this time, whether read or not
            timeout=self._settings.azure_sdk_timeout_seconds, # HTTP request timeout for this SDK call
        )

        message_id = getattr(result, "id", "") or ""  # Extract the Azure-assigned message ID from the response
        logger.info(
            "Message sent",
            extra={"queue": self._settings.queue_name, "message_id": message_id},
        )
        return message_id

    def receive_messages(self) -> Iterable[Any]:
        """Pull messages from the queue. Returns an iterable of raw SDK message objects."""
        self.ensure_queue_exists()

        return self._queue_client.receive_messages(
            messages_per_page=self._settings.max_messages_per_poll,         # How many messages to fetch per batch
            visibility_timeout=self._settings.visibility_timeout_seconds,   # Seconds the message stays invisible to other consumers after being read
            timeout=self._settings.azure_sdk_timeout_seconds,              # HTTP request timeout
        )

    def delete_message(self, *, message_id: str, pop_receipt: str) -> None:
        """Permanently remove a message from the queue using its ID and pop receipt."""
        self._queue_client.delete_message(
            message_id=message_id,     # Which message to delete
            pop_receipt=pop_receipt,    # Proof that we currently hold the lease on this message
            timeout=self._settings.azure_sdk_timeout_seconds,
        )

    def to_received_message(self, msg: Any) -> ReceivedMessage:
        """Convert a raw SDK message object into our ReceivedMessage dataclass."""
        raw_text = getattr(msg, "content", "")  # The raw string stored in the queue
        json_body: Optional[Dict[str, Any]]

        try:
            parsed = json.loads(raw_text)       # Attempt to parse the content as JSON
            # If the top-level value is a dict, use it directly; otherwise wrap it
            json_body = parsed if isinstance(parsed, dict) else {"value": parsed}
        except Exception:
            json_body = None                    # Content is not valid JSON

        return ReceivedMessage(
            message_id=str(getattr(msg, "id", "")),
            dequeue_count=int(getattr(msg, "dequeue_count", 0) or 0),
            raw_text=raw_text,
            json_body=json_body,
        )

    def safe_delete(self, msg: Any) -> None:
        """Delete a message, catching and logging any Azure errors instead of raising."""
        try:
            self.delete_message(message_id=msg.id, pop_receipt=msg.pop_receipt)
        except AzureError:
            # Log the error but don't crash – the message will become visible again after the visibility timeout expires
            logger.exception(
                "Failed to delete message",
                extra={"queue": self._settings.queue_name, "message_id": getattr(msg, "id", "")},
            )
