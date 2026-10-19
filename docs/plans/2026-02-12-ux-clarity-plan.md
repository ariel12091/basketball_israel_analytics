# UX Clarity Plan (First-Time + Non-Analytics Users)

Date: 2026-02-12
Scope: `app/` Shiny UI/UX only (no metric-definition changes, no DB/schema changes)

## Why this plan

The app is powerful but dense: many filters, advanced stat language, and multiple view modes before users understand how to read outputs. The goal is to reduce cognitive load without sacrificing analyst depth.

## Design Principles

1. Keep critical instructions visible; use tooltips for supplementary help only.
2. Use progressive disclosure: show essentials first, reveal advanced controls when needed.
3. Define terms in context (Net, PPP, TS%, OREB%, TOV%, FTR, GN, clutch).
4. Keep one obvious "happy path" per tab for first-time success in under 30 seconds.
5. Protect performance: no additional heavy queries on initial load.

## Current-State Pain Points

1. Filters are comprehensive but intimidating at first glance.
2. Statistical terms are not consistently explained where decisions are made.
3. Empty or confusing states (e.g., no rows due to strict filters) are not highly guided.
4. New users have no orientation on what each tab is for and when to use it.
5. Metric interpretation (good/bad direction, sample-size caveats) is scattered.

## Implementation Plan

## Phase 1 (High impact, low risk) - Inline clarity + onboarding shell

### 1. Add tab-level "What this tab answers" explainer blocks

- Add a compact info card at top of each tab main panel:
  - Tab 1: "Which players change team offense/defense most when on court?"
  - Tab 2: "Which player combinations work best, in what sample?"
  - Tab 3: "How strong are teams overall and by Four Factors?"
  - Tab 4: "How did each game look, with quick drill-down context?"
- Include 2-3 bullet reading instructions per tab.
- Add a dismiss/minimize toggle (session-only) so analysts can collapse it.

Files:
- `app/R/ui_tab1_onoff.R`
- `app/R/ui_tab2_lineup.R`
- `app/R/ui_tab3_team.R`
- `app/R/ui_tab4_gamelogs.R`
- `app/R/global.R` (shared CSS for explainer card)

### 2. Add a global "How to use this app" quick-start popover

- Add an info button near navbar title.
- Popover content:
  - Step 1: pick tab by question
  - Step 2: start broad filters
  - Step 3: tighten filters and watch possessions/sample size
  - Step 4: switch Summary/Four Factors for "what" vs "why"
- Add "Don't show again this session" checkbox.

Files:
- `app/app.R`
- `app/R/global.R`

### 3. Improve label microcopy on high-confusion filters

Rename labels for plain language while preserving IDs:
- "GN" -> "Game Number (GN)"
- "Last N games" -> "Last N Team Games"
- "Min possessions (sum of Off/Def)" -> "Minimum possessions (Off + Def)"
- "Min ON possessions (eligibility)" -> "Minimum ON possessions (for ranking)"

Add one-line helper text under key controls (not tooltips) when task-critical.

Files:
- All `ui_tab*.R` files

### 4. Add term glossary drawer (non-blocking)

- Add right-side collapsible drawer or modal with short definitions:
  - PPP, Net Rating, TS%, OREB%, TOV%, FTR, Possessions, Clutch filter logic
- Link glossary from every tab header.

Files:
- `app/app.R`
- `app/R/global.R`
- optional helper in a small shared UI function file

## Phase 2 (Guidance behavior) - Better defaults and empty states

### 5. Strong empty-state guidance in each DT output

When result has zero rows, show friendly guidance above table:
- "No rows matched these filters. Try:"
  - widen date range
  - clear opponent strength
  - lower min possessions
  - disable clutch filter

Implementation: lightweight conditional UI/text based on existing reactive outputs (no new DB query).

Files:
- `app/R/server_tab1.R`
- `app/R/server_tab2.R`
- `app/R/server_tab3.R`
- `app/R/server_tab4.R`

### 6. Add "Recommended starter preset" per tab

- Add button: "Starter view" that applies beginner-safe defaults.
- Example Tab 1 starter:
  - Summary mode
  - broad date range
  - no opp strength filters
  - min possessions moderate
- Keep existing reset buttons for power users.

Files:
- `app/R/ui_tab*.R`
- `app/R/server_tab*.R`

### 7. Visual cues for filter strictness

- Add small "Filter strictness" chip: Low/Medium/High.
- Derive from count of active restrictive filters (opp strength, clutch, many team/opponent filters, high min poss).
- Helps users understand why rows disappeared.

Files:
- `app/R/server_tab*.R`
- `app/R/global.R`

## Phase 3 (Polish + trust) - Interpretation support

### 8. Column-header tooltips/popovers for advanced metrics

- Add info icons in headers for TS%, OREB%, TOV%, FTR, Net.
- Keep to 1 sentence; longer context goes to glossary.
- Ensure keyboard-focusable triggers.

Files:
- `app/R/server_tab*.R` DT rendering sections

### 9. Add sample-size confidence messaging

- Reuse existing possession thresholds and rank baseline logic.
- Show compact badge:
  - "High sample" / "Moderate sample" / "Small sample - interpret carefully"
- Place near legends and in row tooltips where possible.

Files:
- `app/R/server_tab1.R`
- `app/R/server_tab2.R`
- `app/R/server_tab3.R`

### 10. Human-readable interpretation row under legends

Examples:
- "Green Net means better team performance with this player on floor."
- "In defense columns, lower PPP is better."
- "TOV% is reversed polarity (lower is better on offense)."

Files:
- `app/R/ui_tab1_onoff.R`
- `app/R/ui_tab2_lineup.R`
- `app/R/ui_tab3_team.R`

## Inspiration Mapping (CleaningTheGlass / DunksAndThrees style)

1. Emphasize interpretation over raw stat dumps.
2. Keep clear "what this means" text near metric displays.
3. Use percentile/rank framing with explicit caveats on low sample.
4. Prefer clean, scan-friendly information hierarchy before adding more controls.

## Performance Guardrails

1. No new queries on app startup.
2. All explainer/tooltips/glossary content is static UI text.
3. Any dynamic helper state must reuse existing reactives.
4. Keep table render options unchanged unless needed for UX text only.

## Acceptance Criteria

1. First-time user can answer "what does this tab do" without external help.
2. First-time user can produce non-empty results in each tab within 30 seconds.
3. Empty results always provide next-step recovery guidance.
4. Advanced metric definitions available within one click from every tab.
5. No regression in first-render timing compared with current build.

## Suggested Delivery Order

1. Phase 1 items 1-3 (quick wins)
2. Phase 2 item 5 (empty states)
3. Phase 1 item 4 (glossary)
4. Phase 3 items 8 and 10
5. Phase 2 items 6-7 and Phase 3 item 9

## Rollout and Measurement

1. Release behind a small UI flag if you want rapid rollback.
2. Run a short usability pass (3-5 users): first task success + confusion points.
3. Track support questions and "no rows" frequency before/after.

## References

- USWDS tooltip guidance: use for brief, non-critical information only
  - https://designsystem.digital.gov/components/tooltip/
- Posit bslib tooltip/popover patterns for Shiny
  - https://rstudio.github.io/bslib/articles/tooltips-popovers/index.html
  - https://shiny.posit.co/r/components/display-messages/tooltips/
  - https://shiny.posit.co/r/components/display-messages/popovers/
- Cleaning the Glass style emphasis on interpretation and guides
  - https://cleaningtheglass.com/stats
  - https://www.cleaningtheglass.com/stats/guide/games
  - https://www.cleaningtheglass.com/stats/guide/league_four_factors
- Dunks & Threes style emphasis on explanatory framing + interactive dashboards
  - https://dunksandthrees.com/
