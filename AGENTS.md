# AGENTS.md

This file provides operational guidance for Codex when working in this repository.

## Scope
- Prefer the project documentation in `PROJECT.md` (copied from `CLAUDE.md`) for domain knowledge.
- Do not modify `CLAUDE.md`; keep it as a historical reference.

## Workflow
- Read only what is needed to answer the task.
- Make small, testable changes and explain assumptions.
- Avoid broad refactors unless requested.

## Execution
- Use `Rscript` via the configured path in `PROJECT.md` for running or deploying the app.
- For database changes, follow the Supabase/DDL notes in `PROJECT.md`.

## Output
- Keep responses concise and actionable.
- Cite file paths when changing code.
