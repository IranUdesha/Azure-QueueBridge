# ---------------------------------------------------------------------------
# Dockerfile – Receiver Worker
#
# Builds a lightweight container that runs the long-polling receiver worker.
# The worker continuously reads messages from an Azure Storage Queue.
# ---------------------------------------------------------------------------

# Use the official slim Python 3.12 image (minimal OS footprint)
FROM python:3.12-slim

# PYTHONDONTWRITEBYTECODE=1  – Don't create .pyc files (not needed in containers)
# PYTHONUNBUFFERED=1         – Print logs immediately (don't buffer stdout/stderr)
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

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

# Start the receiver worker directly with Python
CMD ["python", "receiver_worker.py"]
