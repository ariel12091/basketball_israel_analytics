# Clickable cues specification review

Date: 2026-10-11
Reviewed spec: [2026-10-11-clickable-cues-design.md](2026-10-11-clickable-cues-design.md)
Status: review notes for consultation; these suggestions have not been incorporated into the spec.

## Assessment

The spec addresses most launch priorities from the discoverability audit: name menus, Home cards and storylines, lineup drilldowns, Compare rows, editable filter chips, and lineup filter tokens. It also identifies an important issue understated in the original audit: desktop view modes are hidden behind navbar hover menus.

The direction is sound, but the corrections and omissions below should be resolved before treating the design as complete. This was a code-based review, including checks against main for the header and lineup-link selectors. Browser launch failures prevented fresh desktop and phone visual verification.

## Required corrections and additions

### 1. Correct the table-header explanation selector

The spec targets th[data-tooltip], but HEADER_TOOLTIP_JS writes table explanations to th[title]. The label helper tt() uses data-tooltip; table headers use a different mechanism. As written, the new header cues may not appear.

Cover nonempty th[title], preserve existing touch information buttons, and avoid interfering with sorting indicators.

References: design spec line 113; app/R/global.R, HEADER_TOOLTIP_JS around line 224.

### 2. Include European lineup links

The spec names only .ld-lineup-link. European lineup tables use .euro-ld-unit. Both selectors need the same Open cue and consistent link styling. The statement that lineup links are already equally clear across both leagues is incomplete.

References: design spec line 96; app/R/helpers.R, lineup table specs around lines 3880 and 3888.

### 3. Specify Game Flow cues and shared-player discovery

Game Flow is inventory item 9, but has no assigned cue in the vocabulary and no specific implementation section.

Add visible cues on selectable player names, lanes, and detail rows. Explicitly tell users that selecting two or more players reveals shared court time and shared lineups. Existing instructions say to select players but do not clearly advertise that outcome. Preserve the shared-lineup instructions already shown after that section appears.

References: app/R/mod_ribbon_modal.R, ribbon_pair_controls(); app/www/app.js, renderSharedLineups() around line 650.

### 4. Add the stat-filter entry point to the inventory

The generic + Filter control opens column/operator/value filtering, but this entry point is absent from the inventory.

Proposed wording: + Stat filter. Optionally provide an example such as PTS >= 15. Keep it distinct from game-context filters and active-chip editing.

Reference: app/R/helpers.R, stat_filter_chips_ui() around line 569.

### 5. Cover the Net RTG number as well as its tags

The spec adds cues to the 3PT luck and small-sample tags. The Net RTG number itself also opens explanations.

Include the interactive number wherever an .onoff-net-tip target exists. Avoid marking noninteractive numbers or duplicating info indicators on nested number/tag targets.

References: app/R/helpers.R, On/Off Net renderer; app/www/app.js, Net tooltip handler around line 545.

### 6. Make touch instructions match the actual action

Hover a header for its definition cannot simply become Tap a header: tapping a sortable header sorts; tapping its information button explains the column.

Use wording such as Tap the info button for definitions. Select touch wording through existing touch/device classification, including tablets, rather than only a phone-width condition. Preserve the capture-phase handling that prevents an information tap from also sorting.

References: design spec lines 123-125; app/www/mobile.js around lines 767 and 1000.

### 7. Separate menus, cycling choices, and sorting cues

The vocabulary says each cue has one meaning, but the Menu caret is assigned to menu opening, choice cycling, and Gap sorting. Those actions differ.

Reserve a caret for menus, use a sort indicator for sorting, and explicitly define the indicator for cycling a choice. Keep the vocabulary consistent with the one-meaning-per-cue goal.

Reference: design spec vocabulary and remaining menus/toggles sections.

## Additional coverage

- Column reordering: add a desktop hint only on tables and modes that actually support dragging columns. This lower-priority audit finding is omitted. Player Stats enables ColReorder in app/R/server_tab5_traditional.R around line 1162.
- Compare second drilldown: ensure hints and Open cues appear at both the league-row and team-player levels. Selecting a player in the team-player table opens another detailed comparison.
- Deferred inventory items: resolve the to-verify entries before finalizing implementation decisions. Their current appearance is not confirmed by this code review.

## Acceptance-check additions

The desktop/phone browser pass is a useful foundation. Add explicit checks that:

1. Cues remain after sorting, filtering, view changes, and reactive redraws.
2. Explanation-button taps do not accidentally sort a column.
3. Decorative glyphs and hints do not contaminate CSV exports or sortable values.
4. Touch guidance and information controls work on tablets as well as phones.
5. Israeli and European lineup selectors receive equivalent cues; Compare checks apply where Compare is available.
6. Added pseudo-elements do not overwrite sorting indicators or duplicate touch information buttons.

## Conclusion

The spec substantially addresses the audit. Correct the selectors and explicitly specify Game Flow, stat filtering, Net RTG numbers, touch instructions, and distinct cue meanings before implementation. These notes are saved for consultation; the design spec remains unchanged.
