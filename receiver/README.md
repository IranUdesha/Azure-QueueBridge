# Receiver Worker (Azure Storage Queue)

A long-running worker that polls an Azure Storage Queue, prints each message to stdout, and logs it.

## Config

This service reads configuration from environment variables (optionally from a local `.env` loaded by `python-dotenv`).

Required:

- `AZURE_STORAGE_CONNECTION_STRING` (recommended) **or** `AZURE_STORAGE_ACCOUNT_NAME` + `AZURE_STORAGE_ACCOUNT_KEY`
- `AZURE_STORAGE_QUEUE_NAME`

Optional:

- `POLL_INTERVAL_SECONDS`
- `VISIBILITY_TIMEOUT_SECONDS`
- `MAX_MESSAGES_PER_POLL`
- `MAX_DEQUEUE_COUNT`
- `LOG_LEVEL`

## Run locally

From this folder:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
python receiver_worker.py
```

## Run with Docker

From repo root:

```powershell
docker build -t stg-msg-queue-receiver:local -f src/receiver/Dockerfile src/receiver

docker run --rm --env-file src/receiver/.env stg-msg-queue-receiver:local
```
