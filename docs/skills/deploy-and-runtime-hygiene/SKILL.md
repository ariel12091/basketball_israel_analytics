---
name: deploy-and-runtime-hygiene
description: Keep deployment and runtime configuration consistent and safe for the Shiny app. Use for rsconnect deployment issues, environment loading, cert/temp errors, and runtime parity checks.
---

# Deploy and Runtime Hygiene

## Safety Rules

- Never commit secrets or local credential files.
- Use environment variable names only in docs and code snippets.

## Deployment Rules

1. Keep deploy commands path-correct for the app root.
2. Treat rsconnect metadata changes as deploy metadata, not app logic.
3. Validate runtime dependencies before deploy.

## Runtime Hygiene Checks

- `.Rprofile` should not fail if optional files are missing.
- App startup should not depend on local-only paths.
- Certificate/temp file write failures should be treated as environment issues first.

## Debug Workflow

1. Reproduce deploy failure locally with minimal deploy command.
2. Identify if issue is filesystem, cert/temp, package, or auth.
3. Confirm runtime starts with production-like env vars.
4. Re-test deploy after narrow fix.

## Done Criteria

- App starts cleanly in deployment target.
- No local-path assumptions remain.
- No secrets leaked in logs or config.
