# Security policy

## Reporting a vulnerability

Please report suspected vulnerabilities privately through GitHub: open the **Security** tab of this repository and choose **Report a vulnerability**. Do not open a public issue for security problems.

You can expect an acknowledgement, and a fix or mitigation plan once the report is confirmed.

## Scope notes

- API key authentication and TLS are opt-in (`API_KEY`, `TLS_CERT_FILE`, `TLS_KEY_FILE`). Run without them only for local development.
- Authentication is a single shared key, not per-client identity.
- Never commit connection strings, API keys or certificates. `.gitignore` excludes the common cases; rotate any credential that has been exposed.
- Enable Row-Level Security on `ledger_events` and `idempotency_keys` when using Supabase (see the README).
