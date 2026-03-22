# Pre-Publish Checklist

Use this checklist before pushing to a public repository.

## Repository Hygiene

- [ ] .gitignore includes secret and local-only files.
- [ ] No virtual environment or cache artifacts are tracked.
- [ ] README has accurate setup instructions.

## Secrets and Security

- [ ] sender/.env is not tracked.
- [ ] sender/.env.example exists and has placeholder values only.
- [ ] No API keys, connection strings, tokens, or passwords in tracked files.
- [ ] SECURITY.md exists and describes reporting guidance.

## Open-Source Essentials

- [ ] LICENSE is present.
- [ ] CONTRIBUTING.md is present.
- [ ] CODE_OF_CONDUCT.md is present.

## Validation

- [ ] App starts locally with sender/.env.
- [ ] Docker build works from repo root.
- [ ] Basic API check passes (/health and /messages with API key).

## Final Publish Steps

- [ ] Review git diff one more time.
- [ ] Rotate any key that was ever committed accidentally.
- [ ] Push to public repository.
