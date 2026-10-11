# AGENTS.md

This file provides operational guidance for Codex when working in this repository.

## Scope
- Prefer the project documentation in `PROJECT.md` (copied from `CLAUDE.md`) for domain knowledge.

## Workflow
- Read only what is needed to answer the task.
- Make small, testable changes and explain assumptions.
- Ensure every new or changed user-facing feature works on mobile as well as desktop. Verify its layout and interactions at a phone-width viewport before marking the work complete.
- When changing a feature shared by the Israeli league and EuroLeague/EuroCup, check the equivalent behavior in both.
- Avoid broad refactors unless requested.
- Before committing, review the staged diff and include only files related to the requested change.

## Execution
- Use `Rscript` via the configured path in `PROJECT.md` for running or deploying the app.
- For database changes, follow the Supabase/DDL notes in `PROJECT.md`.

## Output
- Keep responses concise and actionable.
- Cite file paths when changing code.
