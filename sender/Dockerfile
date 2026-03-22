# ---------------------------------------------------------------------------
# Dockerfile – Sender API
#
# Builds a lightweight container that runs the FastAPI sender service
# using uvicorn on the port specified by the PORT env var.
# ---------------------------------------------------------------------------

# Use the official slim Python 3.12 image (minimal OS footprint)
FROM python:3.12-slim

# PYTHONDONTWRITEBYTECODE=1  – Don't create .pyc files (not needed in containers)
# PYTHONUNBUFFERED=1         – Print logs immediately (don't buffer stdout/stderr)
# PORT=8000                  – Default port for uvicorn; can be overridden at runtime
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PORT=8000

WORKDIR /app

# Create a non-root user for security (don't run the app as root)
RUN addgroup --system appgroup \
    && adduser --system --ingroup appgroup appuser

# Install Python dependencies first (layer is cached unless requirements.txt changes)
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copy the application source code into the container
COPY . ./

# Switch to the non-root user before running the app
USER appuser

# Document the port the container listens on (informational only)
EXPOSE 8000

# Start uvicorn with the port from the PORT env var
CMD ["sh", "-c", "uvicorn sender_api:app --host 0.0.0.0 --port ${PORT}"]
