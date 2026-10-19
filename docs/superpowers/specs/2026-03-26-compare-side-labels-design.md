# Compare Tab: Side Label Redesign

## Problem

In the Compare tab, it's hard to tell what Side A and Side B represent. Column headers, summary cards, and detail view all use the same verbose `side_summary()` output, which is too long for DT column headers and doesn't leverage preset names.

## Design

### Two label functions

Split `side_summary()` into two functions:

1. **`side_label_short(side)`** — short label for DT column headers and detail grid headers
   - Preset active: return the preset's short name for that side (see table below)
   - Custom mode: return `"A"` or `"B"`

2. **`side_label_full(side)`** — verbose description for summary cards and detail subheader
   - Always builds from actual filter inputs (existing `side_summary()` logic)
   - Used regardless of preset vs custom — the summary cards are the "source of truth" for what each side means

### Preset short labels

| Preset             | Side A         | Side B          |
|--------------------|----------------|-----------------|
| `starters_bench`   | Starters       | Bench           |
| `clutch`           | Clutch         | Non-Clutch      |
| `home_away`        | Home           | Away            |
| `win_loss`         | Win            | Loss            |
| `top_bottom_rank`  | vs Top N       | vs Bottom N     |
| `date_split`       | Before {date}  | From {date}     |
| `gn_split`         | GN 1-{n}       | GN {n+1}+       |

Where N comes from `cmp_a_opp_rank_n` / `cmp_b_opp_rank_n`, {date} from `cmp_split_date`, {n} from `cmp_split_gn`.

Note: `starters_bench` uses "Starters" / "Bench" as simplified labels even though the actual thresholds are >=3 / <=2. The full filter detail is visible in the summary cards via `side_label_full()`.

### Edge cases

- **Players mode:** `cmp_preset` may hold a stale value from a previous Teams/Lineups selection, but presets are hidden in Players mode. `side_label_short()` must check `input$cmp_mode == "Players"` and always return `"A"` / `"B"` (Players mode uses the PvP view, not the DT table, so this mainly affects detail view if reused).
- **Preset + additional filters:** When a user selects a preset then adds extra filters on top, `side_label_short()` still returns the preset label. The extra filters are visible in the summary cards (`side_label_full()`). This is intentional — column headers stay clean.
- **`date_split` / `gn_split` wording:** `side_label_full()` keeps the existing "Before {date}" / "After {date}" phrasing. `side_label_short()` uses "Before {date}" / "From {date}" for A/B respectively. The slight divergence is fine since they serve different purposes.

### Detail view column headers (custom mode)

In custom mode, detail view column headers show just "A" / "B" (not "A · A"). The `"A · "` prefix is only prepended when `side_label_short()` returns something other than the bare letter.

### Label usage by location

| Location                  | Function           | Preset example     | Custom example              |
|---------------------------|--------------------|--------------------|-----------------------------|
| DT column header          | `side_label_short` | "Home"             | "A" / "B" (colored badge)   |
| Summary card title        | `side_label_full`  | "Home"             | "Home . Clutch . vs Top 4"  |
| Detail view column header | `side_label_short` | "A . Home"         | "A" / "B"                   |
| Detail view subheader     | `side_label_full`  | "Home vs Away"     | "Home . Clutch vs Away"     |

### Colored badges in DT column headers (custom mode)

When no preset maps to a short label, columns 3 and 5 get a colored badge via DT `headerCallback` JS:

- **A badge:** background `rgba(123,140,222,.2)`, color `#7b8cde`, border `rgba(123,140,222,.4)`
- **B badge:** background `rgba(232,164,53,.15)`, color `#e8a435`, border `rgba(232,164,53,.35)`

These match the existing sidebar badge styles for A and B.

Badge is a small `<span>` with `border-radius: 50%; padding: 2px 8px; font-size: .7rem; font-weight: 600;` injected into the header cell via `headerCallback`.

When a preset IS active, column headers show the preset label as plain text (no badge needed since the label itself is descriptive).

The `headerCallback` detects badge mode by checking if the column header text is exactly `"A"` or `"B"`. Implementation: `headerCallback = DT::JS(...)` iterating header cells at indices 2 and 4 (0-based), wrapping single-letter content in the badge `<span>`.

## Files to modify

- `app/R/server_tab7_compare.R`:
  - Rename existing `side_summary()` to `side_label_full()`
  - Add new `side_label_short()` function
  - Update `output$cmp_table` to use `side_label_short()` for column names + add `headerCallback` for badges
  - Update `output$cmp_detail_view_ui` column headers to use `side_label_short()`
  - Summary card titles (`cmp_summary_a_title`, `cmp_summary_b_title`) keep using `side_label_full()`
  - Detail subheader keeps using `side_label_full()`

No UI file changes needed — all rendering is server-side.
