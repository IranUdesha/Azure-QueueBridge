# ---------------------------------------------------------------------------
# sender_api.py
#
# A FastAPI HTTP server that exposes a POST /messages endpoint.
# It accepts a JSON body, sends it to an Azure Storage Queue,
# and returns the message ID in the response.
# ---------------------------------------------------------------------------
from __future__ import annotations

import hmac                              # Used for constant-time string comparison in API key validation
import logging                              # Standard library for structured log output
import os                                   # Access environment variables (used in __main__ block)
from contextlib import asynccontextmanager  # Context manager used for FastAPI startup/shutdown lifecycle
from typing import Any, Dict, Optional

from azure.core.exceptions import AzureError  # Base exception class for all Azure SDK errors
from fastapi import FastAPI, HTTPException, Depends  # FastAPI web framework, HTTP error helper, and dependency injection
from fastapi.security import APIKeyHeader             # Reads an API key from a request header
from starlette.requests import Request                # Access the raw request object in dependencies

from classes.queue_client import AzureQueue     # Custom wrapper around the Azure Queue Storage SDK
from config.logging_setup import configure_logging  # Configures log format and log level
from config.settings import load_settings       # Loads environment variables from .env into a Settings object

# Logger instance scoped to this module (uses the module name as the logger name)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Lifespan handler – executed once on application startup.
#
# Steps:
#   1. Load configuration (connection string, queue name, etc.) from .env
#   2. Configure the logging format and level
#   3. Create an AzureQueue client using the loaded settings
#   4. Create the queue in Azure if it does not already exist
#   5. Store settings and queue client on app.state so route handlers can access them
#   6. Build the API-key security scheme using the configured header name
#   7. yield – the app is now running and accepting HTTP requests
#      (any code after yield would run on shutdown)
# ---------------------------------------------------------------------------
@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = load_settings()                  # Read .env and return a validated Settings dataclass
    configure_logging(settings.log_level)       # Set up logging with the configured level (e.g. INFO, DEBUG)

    app.state.settings = settings               # Store settings on app.state for access in route handlers
    app.state.queue = AzureQueue(settings)      # Initialize the Azure Queue client with connection string and retry policy
    app.state.queue.ensure_queue_exists()        # Create the queue in Azure Storage if it doesn't exist yet

    # Create the API key header scheme with the configured header name.
    # auto_error=False so we handle missing/invalid keys ourselves with a 401 (not the default 403).
    app.state.api_key_scheme = APIKeyHeader(
        name=settings.api_key_header_name,
        auto_error=False,
    )

    yield  # Application is running – control returns here when the app is shutting down


# Create the FastAPI application and register the lifespan handler
# so it runs before the first request is served.
app = FastAPI(lifespan=lifespan)


# ---------------------------------------------------------------------------
# API Key validation dependency.
#
# Reads the API key header from the incoming request using the header name
# configured in API_KEY_HEADER_NAME (defaults to "X-API-Key").
# Compares the provided value against the expected key in settings.
# Returns HTTP 401 if the header is missing or the key is wrong.
# Uses hmac.compare_digest for constant-time comparison (prevents timing attacks).
# ---------------------------------------------------------------------------
async def _verify_api_key(request: Request) -> str:
    scheme: APIKeyHeader = request.app.state.api_key_scheme
    api_key: Optional[str] = await scheme(request) # Extract the API key from the request header using the configured scheme

    if not api_key:
        raise HTTPException(status_code=401, detail="Authentication failed")

    if not hmac.compare_digest(api_key, request.app.state.settings.api_key): # Constant-time comparison to prevent timing attacks
        raise HTTPException(status_code=401, detail="Authentication failed")

    return api_key



# Health check endpoint (protected by API key, can be used by orchestrators to verify the API is up)
@app.get("/health", responses={401: {"description": "Invalid or missing API key"}})
def health_check(_key: str = Depends(_verify_api_key)) -> Dict[str, str]: # Validate API Key 
    return {"status": "ok"}


# ---------------------------------------------------------------------------
# POST /messages
#
# Accepts a JSON object in the request body, serializes it, and sends it
# to the Azure Storage Queue. Returns the message ID on success.
# If the Azure SDK raises an error, logs the exception and returns HTTP 502.
# ---------------------------------------------------------------------------
@app.post(
    "/messages",
    responses={
        401: {"description": "Invalid or missing API key"},
        502: {"description": "Failed to enqueue message"},
    },
)
def send_message(payload: Dict[str, Any], _key: str = Depends(_verify_api_key)) -> Dict[str, str]:
    try:
        queue: AzureQueue = app.state.queue        # Retrieve the queue client initialized during startup
        message_id = queue.send_json(payload)       # Serialize the payload to JSON and send it to the queue
        return {"message_id": message_id}           # Return the Azure-assigned message ID to the caller
    except AzureError as exc:
        logger.exception("Failed to send message")  # Log the full traceback for debugging
        raise HTTPException(status_code=502, detail="Failed to enqueue message") from exc


# ---------------------------------------------------------------------------
# Direct execution entry point.
#
# Allows running the API without Docker:
#   python sender_api.py
#
# Reads the PORT from the environment (defaults to 8000) and starts uvicorn.
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    import uvicorn                          # Import here so uvicorn is only needed when running directly

    port = int(os.getenv("PORT", "8000"))
    uvicorn.run("sender_api:app", host="0.0.0.0", port=port)
