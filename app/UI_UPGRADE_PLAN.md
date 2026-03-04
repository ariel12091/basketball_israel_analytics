# Shiny UI Upgrade Plan

## Priority Order — ALL COMPLETE

### 1. Dark Editorial Theme (CSS only) ✅
- Background: Navy-charcoal gradient (`#0d1117` body, `#161b22` cards/sidebar, `#1c2333` table rows)
- Accent: Amber `#e8a435` for active tabs, selected filters, hover states
- Text: `#e6edf3` primary, `#8b949e` secondary
- Table cells: Dark rows with subtle alternating `#141920` / `#1c2333`
- Heat colors already pop more on dark backgrounds
- Implementation: Pure CSS via `bslib::bs_theme()` + `shared_css` updates. Zero server-side changes.

### 2. Tab Icons + Active Tab Animation ✅
- On/Off Impact: toggle-on icon
- Lineup Data: people-fill icon
- Team Ratings: trophy-fill icon
- Game Logs: calendar-event icon
- Player Stats: bar-chart-line icon
- Active tab gets amber underline bar that slides (CSS transition)

### 3. Loading Skeletons ✅
- Skeleton table (gray pulsing rows) while data loads
- Fade-in animation when data arrives (300ms CSS transition)
- Thin amber progress bar below tabs (like YouTube)
- Use CSS keyframes, no extra packages

### 4. Active Filter Chips Bar ✅
- Show active filters as small colored chips between header and table
- Each chip has x button to remove that filter
- "Clear all" when >1 active filter
- Sticky below tab bar
- Color-coded by category

### 5. Global Season Selector ✅
- Season moved to navbar as single global `input$game_year` control
- All 5 tabs read from the same reactive (`selected_game_year()`)
- Per-tab season selectors removed from sidebars
- Date ranges auto-adjust, team/opponent dropdowns reset on season change

### 6. Branded Header ✅
- Navbar: App name + season selector + glossary + last updated indicator
- KPI strip was prototyped but removed (not useful enough to justify the space)

## Implementation Notes
- Items 1-3: Purely additive CSS, zero risk of breaking functionality
- Items 4-6: Touch some reactive logic but well-scoped
- All items preserve current capabilities and performance
