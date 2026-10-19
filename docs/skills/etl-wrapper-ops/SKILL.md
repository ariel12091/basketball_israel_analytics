---
name: etl-wrapper-ops
description: Maintain ETL wrapper reliability across local runs and GitHub Actions. Use for PowerShell argument handling, package parity, log artifacts, dry-run behavior, and environment validation.
---

# ETL Wrapper Ops

## Safety Rules

- Do not hard-code credentials.
- Keep secret values out of logs and artifacts.

## Operational Rules

1. Pass switch parameters only when true.
2. Keep wrapper and workflow package lists aligned.
3. Normalize paths for runner environment.
4. Always emit actionable logs for wrapper start, sub-step failures, and exit code.

## Common Failure Classes

- PowerShell parameter binding and switch conversion errors.
- Missing package parity between local ETL and CI ETL.
- Invalid host/env values (including trailing whitespace/newlines).
- Artifact upload path mismatches.

## Debug Workflow

1. Capture wrapper log and latest ETL log in artifact paths.
2. Identify failing phase: launch, package install, DB connect, ETL execution.
3. Validate env var presence and sanitize string inputs.
4. Re-run dry-run first, then full run.

## Done Criteria

- Dry-run and non-dry-run both execute through wrapper.
- Artifacts always upload expected logs.
- CI behavior matches local ETL requirements.
