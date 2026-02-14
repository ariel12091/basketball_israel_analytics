# Frontend v2 Progress

## Latest Updates (Session 2)

### Column Sorting - FIXED ✅
**Issue:** Sorting didn't work because column index didn't match object property order.

**Solution:** Created proper column-to-key mappings for all 8 table types:
- `onoff-summary` → maps "Net" to `net`, "Player" to `player`, etc.
- `onoff-ff` → maps "Diff" to `netDiff`, Four Factors columns
- `lineups`, `lineups-ff`, `teams`, `teams-ff`, `gamelogs`, `gamelogs-ff`

**How it works:**
1. Click any header (Net, PPP, TS%, Team, etc.)
2. Table sorts by that column's data property
3. Arrow indicator (▲ ▼) shows sort direction
4. Click again to reverse sort
5. Works across all tabs and both Summary/Four Factors modes

### Phase 1 Features Complete

✅ **Column Sorting** (fixed with proper key mapping)
- Handles all column types: numeric, text, nested objects
- Visual indicators update dynamically
- Sort state persists until new column clicked

✅ **CSV Export**
- All CSV buttons functional across all tabs
- Proper filenames: `onoff_summary.csv`, `teams_four_factors.csv`, etc.
- Exports currently displayed data (respects filters)

✅ **Game Logs Team Filter**
- Tab 4 dropdown filters by selected team
- Updates both Summary and Four Factors views
- "All Teams" shows everything

✅ **Keyboard Shortcuts**
- **Escape** closes modal or filter drawer
- Smooth navigation without mouse

### How to Test

1. Open `frontend-v2/index.html` in a browser
2. **Sorting:** Click "Net", "PPP", "Team" — watch data re-sort with arrow indicator
3. **CSV Export:** Click any "CSV" button — downloads current table
4. **Game Filter:** Tab 4 → select team → table filters
5. **Keyboard:** Open drawer/modal → press Escape → closes

### Still TODO from Phase 1

- [ ] Filter drawer → table interaction (team selector in drawer filters Tab 1/2/3)
- [ ] Min possessions slider (live filtering)
- [ ] Group size pills (switch between 2/3/4/5-man lineups)
- [ ] Chip sync (filter changes update chips dynamically)
- [ ] Mobile: collapse filter sections by default

### Code Architecture

**Key Functions:**
- `sortTable(key, dataArray, renderFn, headerEl)` — sorts by data property key
- `makeHeadersSortable()` — sets up click handlers with column→key mappings
- `exportTableCSV(btn)` — detects active tab/mode and exports correct data
- Keyboard listener — handles Escape globally

**Column Mapping Pattern:**
```js
const columnMaps = {
  'onoff-summary': { 'Net': 'net', 'Player': 'player', ... },
  'teams-ff': { 'TS%': 'offTs', 'OREB%': 'offOreb', ... }
};
```

### Next Steps

Phase 1 is ~60% complete. Remaining items:
1. Filter drawer interactions (team/opponent selectors)
2. Min possessions slider with live filtering
3. Group size pills (2/3/4/5-man lineups)
4. Chip sync (dynamic filter chips)
5. Mobile optimizations

Then: **Phase 2 - React Migration** (see PLAN.md)

---

**Files Modified:**
- `index.html` — added sorting, CSV export, keyboard shortcuts, team filter
- `PLAN.md` — updated checklist
- `PROGRESS.md` — this file
