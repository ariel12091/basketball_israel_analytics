# Filter navigation UX — review fixes and Task 6 handoff

**Date:** 2026-09-03  
**Branch:** `shiny/filter-nav-ux`  
**Worktree:** `.worktrees/design-system-pass`

## Completed commits

### `5857f37` — review findings

- Limit filter-sidebar collapsing to panes that contain their own toggle, so
  Compare cannot be left collapsed without a way to reopen it.
- Route filter-chip focus to the real Game Logs, namespaced lineup, and
  opponent-rank input ids.
- Make table pivot cells keyboard operable with Enter/Space, expose button/menu
  semantics, and restore focus when Escape closes the menu.
- Raise secondary heat-cell text to an AA-safe colour.
- Add focused regression coverage for each finding.

Validation completed before the commit:

- Focused filter-collapse, chip-focus, pivot-menu, design-token, and range-cell
  tests passed.
- `scripts/test_all.R` passed with four expected environment/integration skips.
- Modified R files parsed successfully, `node --check app/www/app.js` passed,
  and `git diff --check` was clean.

The full-suite run used the valid Windows locale
`English_United States.utf8`; the ambient `C.UTF-8` value is not valid in this
Windows R installation and was the cause of earlier false Compare failures.

### `217494d` — Task 6 Home navigation rail

- Replace the six Israeli Home question cards with a compact navigation rail
  below the existing team hub.
- Preserve the original `go_*` input ids and `js-shiny-event` behavior, so the
  existing observers and pre-websocket queue/replay path remain unchanged.
- Keep the five EuroLeague Home cards intact because EuroLeague has no team hub
  above them.
- Add responsive, visible-focus, and reduced-motion styling.
- Add Home UI tests for the rail, destination ids, and retained EuroLeague
  cards.

Validation completed:

- `test-team-hub-ui.R` passed all 64 assertions after the Task 6 changes.
- The Task 6 commit records a browser check of the six Israeli rail items,
  accessible nav label, absence of Israeli question cards, and five retained
  EuroLeague cards.
- A second local app/browser run reached `http://127.0.0.1:7667`; its follow-up
  snapshot command was interrupted. The local app has since been stopped.

## Remaining verification

- Run `scripts/test_all.R` once more at `217494d` if a post-Task-6 full-suite
  result is required. The full suite was green immediately before Task 6, and
  the Task 6 focused suite is green.
- Local sandbox browser runs cannot reach Supabase and cannot write the normal
  user-level Sass cache. These are environment limitations, not repository
  changes; no database writes or live loads were performed.

At the time this handoff was written, the two implementation commits were at
the branch tip and the implementation worktree was clean.
