# Security Policy

## Supported Versions

This is a sample project. Security fixes are applied on a best-effort basis to the default branch.

## Reporting a Vulnerability

Please do not open public issues for sensitive vulnerabilities.

Instead:

1. Open a private security advisory in GitHub, or
2. Contact the maintainers directly through a private channel.

Include:

1. A clear description of the issue.
2. Steps to reproduce.
3. Potential impact.
4. Any suggested remediation.

## Secret Handling

1. Never commit real credentials or access keys.
2. Use sender/.env.example as the public template.
3. Keep sender/.env local-only.
4. Rotate any secret immediately if it may have been exposed.
