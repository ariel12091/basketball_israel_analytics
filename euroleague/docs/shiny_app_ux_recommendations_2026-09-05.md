# Local Shiny App UX Recommendations

Date: 2026-09-05

## Scope

This review covers only the local Shiny app in `app/`. It does not assess the
deployed application or either React prototype.

The current app already has a coherent dark visual system, task-oriented Home
cards, filter chips, contextual explainers, loading feedback, and collapsible
filters. The next UX pass should focus on comprehension, trust, and reducing
cognitive load rather than another broad visual redesign.

## Recommended priorities

### 1. Correct the PPP terminology

The interface labels values such as `128.3` as PPP while defining them as
points per 100 possessions. In common basketball usage, that scale is an
offensive or defensive rating; literal PPP would be `1.283`.

Recommended changes:

- Rename `Off PPP` to `Off Rtg`.
- Rename `Def PPP` to `Def Rtg`.
- Rename generic `PPP` columns to `Rating`, or display actual decimal PPP.
- Update explainers, legends, tooltips, glossary entries, and table headers
  together so the terminology remains consistent across tabs and leagues.

This is the clearest low-effort, high-value improvement because it removes a
fundamental interpretation ambiguity.

### 2. Make freshness and data-quality status league-specific

The navbar's green `Last updated` indicator looks authoritative, but the
current fallback query reads the Israeli `basketball_test.app_meta` value even
when the selected league is EuroLeague or EuroCup.

For EuroLeague, show a status such as:

> Data through Round 34 · 306/306 games published · 3 games excluded by QA

The compact navbar label could show the data-through date or round. Selecting
it should open a small status popover with:

- Last successful publication time.
- Latest included game date and round.
- Published versus expected game count.
- QA exclusions and unresolved review count.
- A short explanation of what an exclusion means.

This improves user trust and addresses the existing project gap around QA
visibility.

### 3. Split filters into Essentials and Advanced

The available filters are analytically useful, but presenting the full filter
vocabulary together makes the app appear harder to use than it is. The current
mobile collapse control also hides common and advanced controls together.

Keep these controls immediately visible:

- Date or round range.
- Team.
- Minimum possessions.
- View.
- Reset.

Place clutch, starter context, opponent strength, score state, home/away, and
similar controls under an `Advanced filters` disclosure. Rename `Show Filters`
to a stateful label such as `Filters · 3`, where the count reflects active
non-default filters.

### 4. Improve feedback during expensive requests

The global progress animation indicates activity, but it does not explain what
the application is doing. This matters most for lineup and custom-clutch paths,
which can take several seconds.

Recommended behavior:

- Keep the previous result visible while a replacement is calculated.
- Add a subtle table overlay with a specific message, for example:
  `Updating 5-player lineups · Applying 4 filters`.
- Distinguish loading, empty-result, validation, and database-error states.
- Consider an explicit `Apply filters` button for expensive custom filter
  combinations so several controls can be changed before one query runs.
- Make it clear when displayed results are still based on the previous filter
  state.

### 5. Provide a lower-density default table

The tables are analytically rich, but the combination of offense and defense
blocks, heat colors, secondary values, ranks, arrows, and horizontal scrolling
creates a high initial cognitive load.

The default Summary view should prioritize:

- Player, lineup, or team identity.
- Off Rtg.
- Def Rtg.
- Net Rtg.
- Minutes.
- Possessions.

Put shooting splits and supporting metrics behind `Show details`, a metric
preset, or the existing Four Factors view. Preserve column customization for
advanced users.

On narrow screens, a compact card or selected-metric view would be more usable
than shrinking the complete desktop table and relying on horizontal scrolling.

### 6. Reduce reliance on red-green coloring

Color is currently a strong part of the information hierarchy. Signed values,
ranks, and arrows help, but meaning should remain available without color.

Recommended changes:

- Use a color-blind-safe diverging palette.
- Retain explicit signs, ranks, and direction arrows.
- Reserve saturated colors for meaningful extremes.
- Consider a `Reduced color` display preference.
- Verify text contrast in every heatmap stop, including hover and selected-row
  states.

### 7. Preserve analytical context more visibly

After league and season selection, provide a stable context summary above the
result area, for example:

> EuroLeague · 2025–26 · All teams · Full season

Filter chips should continue to represent removable deviations from that
baseline. A stable context line would improve orientation when users pivot
from a player or team into another tab.

## Suggested implementation sequence

1. Correct PPP/rating terminology everywhere.
2. Add league-specific freshness and QA status.
3. Restructure filters into Essentials and Advanced.
4. Add contextual loading, stale-result, empty, and error states.
5. Reduce the default table density and design a narrow-screen presentation.
6. Audit the heatmap palette and non-color cues.
7. Add a persistent analytical context summary.

The first three items provide the greatest improvement in comprehension and
trust without changing analytical behavior or requiring a new visual identity.

## Relevant local Shiny files

- `../app/app.R`: navbar, league/season selectors, glossary, and freshness
  indicator.
- `../app/R/ui_tab0_home.R`: Home page, league chooser, and task-oriented
  navigation cards.
- `../app/R/ui_tab8_euro.R`: EuroLeague player on/off UI.
- `../app/R/ui_tab9_euro_team.R`: EuroLeague team ratings UI.
- `../app/R/ui_tab10_euro_lineups.R`: EuroLeague lineup UI.
- `../app/R/ui_tab11_euro_gamelogs.R`: EuroLeague game-log UI.
- `../app/www/app.css`: responsive table, filter-chip, loading, and visual
  styling.
- `../app/www/app.js`: league navigation, navbar spacing, filter behavior, and
  table interactions.
