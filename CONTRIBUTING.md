# Contributing

Thanks for your interest in improving Azure-QueueBridge.

## Development Setup

1. Open a terminal in sender.
2. Ensure uv is installed.
3. Install dependencies with uv sync.
4. Copy sender/.env.example to sender/.env and set local values.

## Contribution Guidelines

1. Keep changes focused and small.
2. Never commit secrets, keys, or local .env files.
3. Update documentation when behavior or configuration changes.
4. Preserve compatibility unless a breaking change is clearly documented.

## Pull Request Checklist

1. Code runs locally.
2. README is updated if needed.
3. No secrets are included in commits.
4. .env.example reflects any new environment variables.
5. Commit messages are clear and descriptive.

## Reporting Issues

Please include:

1. What you expected to happen.
2. What actually happened.
3. Reproduction steps.
4. Relevant logs with sensitive values redacted.
