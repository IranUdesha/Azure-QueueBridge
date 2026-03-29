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
import threading                            # Renew message lease while long-running work is in progress
import time                                 # Sleep between polls when the queue is empty

from classes.queue_client import AzureQueue          # Wrapper around the Azure Queue Storage SDK
from config.logging_setup import configure_logging   # Sets up log format and level
from config.settings import load_settings            # Loads .env into a validated Settings dataclass

logger = logging.getLogger(__name__)        # Module-scoped logger


def _start_visibility_renewal(queue: AzureQueue, msg: object, visibility_timeout_seconds: int):
    """Start a background loop that extends message invisibility until stopped."""
    state = {"pop_receipt": str(getattr(msg, "pop_receipt", "") or "")}
    stop_event = threading.Event()
    renew_interval = max(1.0, float(visibility_timeout_seconds) * 0.8)

    def _renew_loop() -> None:
        while not stop_event.wait(renew_interval):
            try:
                state["pop_receipt"] = queue.extend_message_visibility(
                    message_id=str(getattr(msg, "id", "")),
                    pop_receipt=state["pop_receipt"],
                )
                print(f"Extended visibility by {visibility_timeout_seconds} seconds for message {getattr(msg, 'id', '')}", flush=True)  # Visible in stdout/logs
                logger.info(
                    "Message visibility extended",
                    extra={
                        "message_id": str(getattr(msg, "id", "")),
                    },
                )
                print("", flush=True)  # Blank line for log readability
                
            except Exception:
                logger.exception(
                    "Failed to extend message visibility",
                    extra={"message_id": str(getattr(msg, "id", ""))},
                )

    thread = threading.Thread(target=_renew_loop, daemon=True, name="queue-visibility-renewal")
    thread.start()
    return stop_event, thread, state


def main() -> int:
    """Entry point: load config, connect to Azure, and start the poll loop."""

    settings = load_settings()              # Read .env and validate all configuration
    configure_logging(settings.log_level, settings.log_file_path, settings.log_file_name)   # Set up logging and optional file output

    queue = AzureQueue(settings)            # Create queue client with connection string and retry policy
    queue.ensure_queue_exists()             # Create the queue in Azure if it doesn't already exist
    print(f"Listening for messages on queue: {settings.queue_name}", flush=True)  # Initial log to stdout
    logger.info("Receiver started", extra={"queue": settings.queue_name})

    # --- Infinite poll loop ---
    while True:
        received_any = False                # Track whether we got any messages in this iteration

        for msg in queue.receive_messages():
            received_any = True

            # Convert the raw SDK message into our ReceivedMessage dataclass
            received = queue.to_received_message(msg)
            
            # Log the message receipt with structured context (queue name, message ID, dequeue count)
            logger.debug(
                f"Message received: message_id={received.message_id},dequeue_count={received.dequeue_count}",
                extra={
                    "queue": settings.queue_name,
                    "message_id": received.message_id,
                    "dequeue_count": received.dequeue_count,
                },
            )

            # --- Poison message detection ---
            # If the message has been read more times than the threshold,
            # it likely keeps failing. Delete it to stop the retry loop.
            # if received.dequeue_count > settings.max_dequeue_count:
            #     logger.error(
            #         f"Max dequeue count exceeded; deleting message: message_id={received.message_id}",
            #         extra={
            #             "queue": settings.queue_name,
            #             "message_id": received.message_id,
            #             "dequeue_count": received.dequeue_count,
            #         },
            #     )
            #     print(f"Message {received.message_id} exceeded max dequeue count and will be deleted without processing.", flush=True)
            #     # queue.safe_delete(msg)      # Delete without raising on error
            #     continue                    # Skip to next message


            
            # if order id = 1 then print a message
            if (received.json_body or {}).get("order_id") == "1":
                
                # Start background thread to renew message visibility while we process it
                stop_event, renew_thread, lease_state = _start_visibility_renewal(
                    queue=queue,
                    msg=msg,
                    visibility_timeout_seconds=settings.visibility_timeout_seconds,
                )
                processed_successfully = False

                try:
                
                    print("Order ID 1 received!", flush=True)
                    # Print the raw message content to stdout (visible in docker logs / terminal)
                    print(received.json_body, flush=True)
                    logger.info("-----------------------------------------------")
                    logger.info(" ")
                    # wait for 10 seconds to simulate processing time
                    time.sleep(10)
                    processed_successfully = True
                finally:
                    # Stop the visibility renewal thread when processing is done (whether successful or not)
                    stop_event.set()
                    # Wait briefly for the thread to exit before continuing; it will stop renewing the lease on the message
                    renew_thread.join(timeout=1.0)

                if processed_successfully:
                    # Delete only after renewal thread stops to avoid pop-receipt races.
                    queue.safe_delete_by_ref(message_id=msg.id, pop_receipt=lease_state["pop_receipt"])
            else:
                continue


        # If no messages were available, sleep before polling again to avoid busy-waiting
        if not received_any:
            time.sleep(settings.poll_interval_seconds)
            
        # print("--- Polling for new messages ---", flush=True)  # Separator in logs for each poll iteration
        # print("", flush=True)  # Blank line for better readability


if __name__ == "__main__":
    raise SystemExit(main())               # Exit with the return code from main()
