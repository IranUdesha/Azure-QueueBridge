# ---------------------------------------------------------------------------
# Dockerfile – Receiver Worker
#
# Builds a lightweight container that runs the long-polling receiver worker.
# The worker continuously reads messages from an Azure Storage Queue.
# Dependencies are managed via pyproject.toml and installed with uv.
# ---------------------------------------------------------------------------

# Use the official slim Python 3.12 image (minimal OS footprint)
FROM python:3.12-slim

# PYTHONDONTWRITEBYTECODE=1  – Don't create .pyc files (not needed in containers)
# PYTHONUNBUFFERED=1         – Print logs immediately (don't buffer stdout/stderr)
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# Install uv from the official container image
COPY --from=ghcr.io/astral-sh/uv:0.8.17 /uv /bin/uv

# Create a non-root user for security (don't run the app as root)
RUN addgroup --system appgroup \
    && adduser --system --ingroup appgroup appuser

# Copy dependency metadata and install dependencies first for better layer caching
COPY pyproject.toml README.md ./
RUN uv sync --no-dev --no-install-project

# Copy the application source code into the container
COPY . ./

# Install the local project into the environment after source copy
RUN uv sync --no-dev

# Ensure non-root runtime user can write log files under /app/logs
RUN mkdir -p /app/logs \
    && chown -R appuser:appgroup /app

# Switch to the non-root user before running the app
USER appuser

# Start the receiver worker from uv-managed virtual environment
CMD [".venv/bin/python", "receiver_worker.py"]
