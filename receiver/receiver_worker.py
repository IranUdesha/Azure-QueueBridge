# ---------------------------------------------------------------------------
# receiver_worker.py
#
# Long-running worker that continuously polls an Azure Storage Queue for
# messages. Each message is logged, printed to stdout, and then deleted.
# If a message has been read too many times (poison message), it is deleted
# without processing to prevent infinite retry loops.
# ---------------------------------------------------------------------------
from __future__ import annotations

import logging                              # Structured log output
import time                                 # Sleep between polls when the queue is empty

from classes.queue_client import AzureQueue          # Wrapper around the Azure Queue Storage SDK
from config.logging_setup import configure_logging   # Sets up log format and level
from config.settings import load_settings            # Loads .env into a validated Settings dataclass

logger = logging.getLogger(__name__)        # Module-scoped logger


def main() -> int:
    """Entry point: load config, connect to Azure, and start the poll loop."""

    settings = load_settings()              # Read .env and validate all configuration
    configure_logging(settings.log_level)   # Set up logging (e.g. INFO, DEBUG)

    queue = AzureQueue(settings)            # Create queue client with connection string and retry policy
    queue.ensure_queue_exists()             # Create the queue in Azure if it doesn't already exist

    logger.info("Receiver started", extra={"queue": settings.queue_name})

    # --- Infinite poll loop ---
    while True:
        received_any = False                # Track whether we got any messages in this iteration

        for msg in queue.receive_messages():
            received_any = True

            # Convert the raw SDK message into our ReceivedMessage dataclass
            received = queue.to_received_message(msg)
            logger.info(
                "Message received",
                extra={
                    "queue": settings.queue_name,
                    "message_id": received.message_id,
                    "dequeue_count": received.dequeue_count,
                },
            )

            # Print the raw message content to stdout (visible in docker logs / terminal)
            print(received.raw_text, flush=True)

            # --- Poison message detection ---
            # If the message has been read more times than the threshold,
            # it likely keeps failing. Delete it to stop the retry loop.
            if received.dequeue_count > settings.max_dequeue_count:
                logger.error(
                    "Max dequeue count exceeded; deleting message",
                    extra={
                        "queue": settings.queue_name,
                        "message_id": received.message_id,
                        "dequeue_count": received.dequeue_count,
                    },
                )
                queue.safe_delete(msg)      # Delete without raising on error
                continue                    # Skip to next message

            # Message processed successfully – delete it from the queue
            queue.safe_delete(msg)

        # If no messages were available, sleep before polling again to avoid busy-waiting
        if not received_any:
            time.sleep(settings.poll_interval_seconds)


if __name__ == "__main__":
    raise SystemExit(main())               # Exit with the return code from main()
