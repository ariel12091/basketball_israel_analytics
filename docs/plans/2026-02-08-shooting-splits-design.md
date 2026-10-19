# Shooting Splits in Summary Table

## Goal

Add 2PT/3PT shot frequency and accuracy to Tab 1 Summary, as on/off splits (team shooting when player is on vs off court), for both offense and defense.

## Visual Design

4 new stacked-bar columns in the Summary table. Each cell is a horizontal bar split into 2PT (blue) and 3PT (orange) segments:

- **Segment width** = frequency (% of total FGA that are 2PT vs 3PT)
- **Label** = FG% (accuracy)
- If segment < 15% width, label moves outside the bar
- 0 attempts for a shot type = no segment shown

### Table Layout

```
         | Net Impact | On Court Stats                        | Off Court Stats                       | Usage
Team Player| Net Off Def | Off PPP Def PPP Net  Off Shot Def Shot | Off PPP Def PPP Net  Off Shot Def Shot | On  Off
```

4 new columns: Off Shot ON, Def Shot ON, Off Shot OFF, Def Shot OFF.

## Data Model

### Raw counts (16 new columns)

Naming follows existing `off_on_*` / `def_on_*` / `off_off_*` / `def_off_*` convention:

```
off_on_fg2_made, off_on_fg2_att, off_on_fg3_made, off_on_fg3_att
off_off_fg2_made, off_off_fg2_att, off_off_fg3_made, off_off_fg3_att
def_on_fg2_made, def_on_fg2_att, def_on_fg3_made, def_on_fg3_att
def_off_fg2_made, def_off_fg2_att, def_off_fg3_made, def_off_fg3_att
```

Source: `df_pts_poss_lineups_longer_mv` — `parameters_points` (2/3), `parameters_made` ('made'/'missed'), `type = 'shot'`.

### Derived display values (computed in R)

- `fg2_pct = fg2_made / fg2_att`
- `fg3_pct = fg3_made / fg3_att`
- `fg2_freq = fg2_att / (fg2_att + fg3_att)`
- `fg3_freq = fg3_att / (fg2_att + fg3_att)`

## Pipeline Changes

### 1. `onoff_default_mv` (L2) — MV path

Add 16 columns with CASE expressions aggregating from `df_pts_poss_lineups_longer_mv`.

### 2. `player_onoff_by_game` (L3) — base for dynamic path

Add same 16 columns, aggregated per game.

### 3. `onoff_compute()` function — dynamic path

SUM the 16 columns from `player_onoff_by_game`. No new inline CASE logic.

### 4. R `server_tab1.R`

- Derive FG% and freq in both MV and dynamic reactive paths
- Add 4 columns with JS `columnDefs.render` for stacked bar visualization
- Update header sketch (colspan adjustments)
- Add CSS for bar colors/sizing

## Implementation Order

1. Read existing MV SQL for exact aggregation patterns
2. Add columns to `player_onoff_by_game` MV
3. Add columns to `onoff_default_mv` MV
4. Update `onoff_compute()` function
5. Deploy to Supabase (DROP + CREATE, L2 then L3)
6. Refresh MVs
7. Update R server + rendering
8. Test locally
