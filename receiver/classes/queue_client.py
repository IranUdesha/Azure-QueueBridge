# ---------------------------------------------------------------------------
# queue_client.py  (receiver)
#
# Wrapper around the Azure Storage Queue SDK.
# Provides methods to send JSON messages, receive messages, and delete them.
# Includes a monkey-patch for an Azure SDK bug (see below).
# ---------------------------------------------------------------------------
from __future__ import annotations

import json                                          # Serialize/deserialize message payloads to JSON
import logging                                       # Structured log output
from dataclasses import dataclass                    # Immutable data container for received messages
from datetime import timedelta                       # Used to express message TTL
from typing import Any, Dict, Iterable, Optional

from azure.core.exceptions import AzureError, ResourceExistsError  # Azure SDK exception types
from azure.core.pipeline.policies import RetryPolicy               # Configures automatic retries on transient failures
from azure.storage.queue import QueueClient, QueueServiceClient    # Azure Queue Storage SDK clients

from config.settings import Settings                 # Validated configuration loaded from .env

logger = logging.getLogger(__name__)                 # Module-scoped logger

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
        """Create the queue in Azure if it doesn't already exist (idempotent)."""
        try:
            self._queue_client.create_queue(timeout=self._settings.azure_sdk_timeout_seconds)
            logger.info("Queue created", extra={"queue": self._settings.queue_name})
        except ResourceExistsError:
            # Queue already exists – this is fine, just log and move on
            logger.debug("Queue already exists", extra={"queue": self._settings.queue_name})

    def send_json(self, payload: Dict[str, Any]) -> str:
        """Serialize a dict to JSON and send it to the queue. Returns the message ID."""
        self.ensure_queue_exists()              # Make sure the queue exists before sending

        # Compact JSON – no extra whitespace, non-ASCII characters preserved
        message_text = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
        ttl = timedelta(seconds=self._settings.message_ttl_seconds)  # TTL as timedelta for the SDK

        result = self._queue_client.send_message(
            message_text,
            time_to_live=ttl,                   # Azure auto-deletes the message after this time, whether read or not
            timeout=self._settings.azure_sdk_timeout_seconds,  # HTTP request timeout for this SDK call
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
            message=message_id,         # QueueClient expects the message ID as `message`
            pop_receipt=pop_receipt,    # Proof that we currently hold the lease on this message
            timeout=self._settings.azure_sdk_timeout_seconds,
        )
        print(f"Message deleted: message_id={message_id}", flush=True)  # Visible in stdout/logs
        logger.info(
            "Message deleted",
            extra={"message_id": message_id},
        )


    def extend_message_visibility(self, *, message_id: str, pop_receipt: str) -> str:
        """Extend message invisibility by VISIBILITY_TIMEOUT_SECONDS and return the new pop receipt."""
        updated = self._queue_client.update_message(
            message=message_id,
            pop_receipt=pop_receipt,
            visibility_timeout=self._settings.visibility_timeout_seconds,
            timeout=self._settings.azure_sdk_timeout_seconds,
        )

        # Azure rotates pop receipts after update_message; keep the latest for delete.
        return str(getattr(updated, "pop_receipt", "") or pop_receipt)

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
        self.safe_delete_by_ref(message_id=msg.id, pop_receipt=msg.pop_receipt)

    def safe_delete_by_ref(self, *, message_id: str, pop_receipt: str) -> None:
        """Delete by message ID + pop receipt while swallowing recoverable SDK/runtime errors."""
        try:
            self.delete_message(message_id=message_id, pop_receipt=pop_receipt)
        except AzureError:
            # Log the error but don't crash – the message will become visible again after the visibility timeout expires
            logger.exception(
                "Failed to delete message",
                extra={"queue": self._settings.queue_name, "message_id": message_id},
            )
        except Exception:
            # Defensive guard so unexpected SDK/runtime errors don't terminate the long-running worker.
            logger.exception(
                "Unexpected error while deleting message",
                extra={"queue": self._settings.queue_name, "message_id": message_id},
            )
