# Phase 0 execution status

User instruction (2026-10-20): finish Phase 0 implementation using the current roles;
document progress when approaching the five-hour execution limit.

## Current authorization and constraints

- Use the existing `app_readonly` web/app login and the existing ETL owner for DDL.
  Do not create `app_web`, rotate passwords, change memberships, connection limits,
  or role-default settings. Add only grants required by the additive API/publication objects.
- Keep Shiny operational. Do not run a production game load to test publication code.
- Implement against PostgreSQL 17.4 (verified live), rather than the plan's older 15 label.
- No deployment or load-test success is claimed until verified against an actual host.

## Baseline

Task 1 implementation was committed as `a352e98` but not deployed. The existing login
passed live read-only checks: schedule readable; INSERT, CREATE, unapproved function and
privileged role access denied; independent security audit returned zero violations.
Neither `app_api` nor `app_web` existed at the start. Existing role defaults were a
two-minute statement timeout and no idle transaction timeout; web deadlines must be
enforced per request, without altering these role defaults.

## Work in progress

Execution clock: goal activated at 2026-10-20 06:58 UTC (09:58 Israel). If still running,
write the detailed handoff before 11:58 UTC (14:58 Israel).

- Additive SQL API, deployment scripts, security allowlists, and proof tooling.
- Publication SQL and Israeli writer lifecycle with isolated tests.
- EuroLeague/EuroCup publication lifecycle, including shared refreshes affecting both.
- Next.js application, faithful Game Logs rules, offline goldens, and responsive UI.

This file will record validation results, remaining work, and an explicit handoff if the
five-hour limit is approached.
