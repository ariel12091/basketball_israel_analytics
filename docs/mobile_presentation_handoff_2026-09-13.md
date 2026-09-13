# Mobile presentation — handoff, 2026-09-13

**Branch:** `shiny/mobile-presentation` (not merged, not deployed)
**Spec:** `docs/superpowers/specs/2026-09-13-mobile-presentation-design.md`
**Plan:** `docs/superpowers/plans/2026-09-13-mobile-presentation.md`
**Rework brief (authoritative over the plan):** `.superpowers/sdd/2026-09-13-mobile-presentation/rework-brief.md`
**Full decision ledger, every ruling:** `.superpowers/sdd/2026-09-13-mobile-presentation/progress.md`

Merging this branch **is deploying** — Connect Cloud builds from `main`.

---

## Completion update

The interrupted column-header info refactor is committed as `04dca1a`.
Its focused mobile test file passed, JavaScript syntax checked, and the app
started locally. A 390px browser check confirmed the mobile navbar and a
season switch to 25-26; it did not cover all tabs or the header-info tap.
Further device verification is left to the user. This branch remains unmerged
and undeployed.

---

## What this is

A mobile presentation layer for the Shiny app. **Functionality is frozen** — no
metric, query, MV or Shiny input changed. The whole layer is two client files
plus four small R edits, all committed.

- `app/www/mobile.css` and `app/www/mobile.js` — everything, scoped to a
  `body.ibpl-mobile` class set from `matchMedia("(max-width: 767.98px)")`
- `app/app.R` — two guarded `include*` calls, and `collapsible = TRUE`
- `app/R/global.R` — `IBPL_MOBILE` env flag, and `maximum-scale=1` dropped from
  the viewport meta so pinch-zoom works
- `app/R/ui_tab7_compare.R` — one `class = "cmp-summary-row"` CSS hook

**Kill switch:** `IBPL_MOBILE=false` skips both includes. It is documented in
`CLAUDE.md`'s Connect Cloud optional-env list and the mobile files are in its
file map.

**While editing `mobile.css`/`mobile.js`, run with `IBPL_CACHE_UI=false`** or
edits need an app restart rather than a browser reload.

---

## Current state

| Commit | What |
|---|---|
| `88fec43` `02e18c9` | Foundation: mode signal, kill switch, viewport fix |
| `8fb5875` `cb9da83` | Moved three legacy `@media` blocks into `mobile.css` |
| `3485a41` `a051198` `92a2824` | Table layer *(largely removed later — see rework)* |
| `682a1f5` `91fba10` | Collapsible navbar, cluster relocation, view-mode promotion |
| `038a440` `85bd7ca` | Bottom-sheet component *(removed later)* |
| `0e2a2ef` `9999975` | Full-screen modals, tap tooltips, popover relocation |
| `3552ebc` | Compare: two-up summary, column override *(override removed later)* |
| `d0c7794` | Gameflow: ribbon pinned to designed scale |
| `8608153` | **Rework R1:** every column shown, identity column pinned |
| `1eb95a3` | **Rework R2-R5:** filters and view mode inline, never overlays |
| `04dca1a` | **Rework R6:** header and label explanations inline; 44px header-info target; sheet removed |

The focused `test-mobile-layer.R` suite passed after `04dca1a`. The browser
check was limited as described above.

---

## The design reversal — essential context

The layer was built to an approved design, then **tested on a real phone and
two core decisions were rejected**:

- tap-to-expand tables (3 priority columns + caret) → **all columns, identity
  column pinned, horizontal scroll**
- filter bottom sheet → **inline expansion in the page flow**

Plus: view-mode radios moved into the burger menu, min-poss demoted, and
"avoid popups as much as possible on mobile".

Commits `8608153`, `1eb95a3`, and `04dca1a` are that rework. It is mostly deletion —
roughly 680 lines removed.

**Do not reintroduce the removed mechanisms.** They were not abandoned as
unfinished; they were built, reviewed, verified and then rejected in the hand.

---

## What remains for user verification

- On a phone, check that tapping the column-header info target shows its
  explanation inline without sorting, and that sidebar label explanations
  also appear inline. The completed browser check did not exercise these taps.
- The surviving Compare two-up summary and Gameflow ribbon width pin have no
  independent review sign-off. Check their legibility on the device if relevant.
- The full 11-tab, multi-width Task 9 sweep was not completed. Decision F36 in
  the progress ledger records that the user accepted dropping that sweep after
  phone testing; it is not a merge requirement.

Use season 25-26 when checking data-bearing tables: the default 26-27 season
has only 10 games and can leave tables empty. Check legibility as well as
overflow; an SVG can shrink its labels without overflowing.

---

## How to test on a real device

```bash
# On the PC, from the repo root:
IBPL_CACHE_UI=false "/c/Program Files/R/R-4.4.2/bin/Rscript.exe" -e "shiny::runApp('app', port=7799, host='0.0.0.0', launch.browser=FALSE)"
```

Then browse to `http://<PC-LAN-IP>:7799` on the phone. **Switch the season to
25-26 first** or half the tables come up empty — that is real data, not a bug.

---

## Hazards — seven regressions came from these, none visible in a diff

**Event and DOM ownership is the theme of this entire layer.**

1. **Bind in CAPTURE phase when another library owns a handler closer to the
   target.** Three separate conflicts all resolved this way. DataTables binds
   sort directly on the `th`; Compare binds `table.on('click','tbody tr')` on
   the table (`server_tab7_compare.R:3324`, `:3980`); Bootstrap binds popover
   triggers on the trigger. A `$(document).on(...)` bubble handler **cannot**
   pre-empt any of them — it only adds. A caret's `stopPropagation()` was inert
   for this reason and fired Compare's detail view on every tap.
2. **Before binding a delegated handler, enumerate what else matches the
   selector and what those elements already do.** A `[data-tooltip]` handler's
   `preventDefault()` silently swallowed checkbox toggles, because `tt()`
   (`global.R:209`) wraps control labels in `span[data-tooltip]`. There is a
   scoped test asserting that `preventDefault` stays absent — keep it.
3. **Never broadcast a synthetic event document-wide without checking every
   listener it reaches.** On `document`: `keydown` at `app.js:896` (ribbon, has
   an `Escape` branch clearing selected lanes), `:2071`, `:2175` (pivot);
   bubble `click` at `app.js:2162` (pivot open + outside-click dismiss).
4. **A synthetic `.click()` always lands dead centre, so it cannot reproduce a
   missed tap.** That is how a 14px control passed every automated check.
   Test near the EDGES of a hit area, and check side effects (did it sort?).
5. **Four controls in this layer existed but did not render** — hidden by
   `app.css:965` (`.view-mode-container`), `:969` (`#cmp_mode`), and an inline
   `display:none` wrapper (`#ts_display_mode`). Presence in the DOM proves
   nothing. Verify a control is **visible and drives a re-render**.
6. **`app.R` and `global.R` have MIXED line endings and it is load-bearing.**
   `app.R` 678 lines / 355 CR; `global.R` 1560 / 1030. **`grep -c $'\r'` and
   `cat -A` report 0 CRs in these files and lie.** Use
   `perl -ne 'print "$.\n" if /\r\n$/'` and `tr -cd '\r' | wc -c`. Edit with
   `perl -i`, never the Edit tool, and diffstat before committing.
7. **An unexplained crash is contained, not solved.** A
   `TypeError: ... reading 'querySelectorAll'` at `removeCarets` was seen twice
   live by different agents; three static readings failed to find the path. A
   null guard contained it. That code is now deleted with the caret layer, so
   it should be moot — but if anything like it recurs, capture `typeof node`
   and `node.nodeType` at the crash site, which nobody ever got.

---

## Pre-existing failures — not caused by this work

- `test-idle-restore-bookmarking.R:169`
- `test-companion-query-counts.R:293`

Both verified to fail identically at the branch fork point `b98ccb8`.
`test-deployed-app-smoke.R` is separately known-stale.

Known console noise on Home: `hub_storylines` output-state errors, favicon 404,
datepicker-locale warnings.

---

## Honest assessment

The review process caught roughly twelve implementation defects and every one of
the seven event-ordering regressions. **It caught none of the four design
defects.** Those came from forty minutes with a phone. Nothing in a diff answers
"does this feel right in the hand" — so if you continue this work, get it onto a
device early and often rather than trusting a green suite.
