# API / MCP: feature inventory of the Shiny app (2026-10-19)

The new API and MCP server must keep every feature listed here. Each row has an ID; the
parity plan (`2026-10-19-api-mcp-design.md`) gives every ID an endpoint, a parameter or a
response field, and a test that proves the API returns what Shiny shows.

Source: `origin/main` at `c6dc605`. Every server file was read in full: Tabs 1-5, 7-11,
`mod_team_hub.R`, `mod_ribbon_modal.R`, `mod_lineup_player_filter.R`, plus the shared
renderers in `helpers.R`, `global.R`, `global_euro.R` and `www/app.js`. Tab 6
(`server_tab6_team_stats.R`) is on disk but not sourced by `app.R`, so it is out of scope.
The deprecated Plumber API in `frontend-v2/server` was not used as a reference.

Legend for "Where it runs" -- where the logic lives today, which decides how the API can
reuse it:
- **SQL**: a stored function or view; the API calls it as Shiny does.
- **R-pure**: a top-level function in `helpers.R`/`global.R`; the API can call it directly.
- **R-closure**: logic inside a Shiny server function or reactive; it must be extracted
  into a pure function first, or the API would have to copy it (forbidden by rule R1).
- **Client**: browser-side (DataTables / `app.js`); the API must return what the client
  needs to reproduce it.

## X. Cross-cutting (every tab)

| ID | Feature | Where it runs |
|---|---|---|
| X-01 | League switch: Israeli (`il`) vs EuroLeague / EuroCup (`E`, `U`); league-local ids never cross over | Client + R-closure |
| X-02 | Israeli season = season-ending year (`DEFAULT_GAME_YEAR` 2027); EuroLeague season = provider start year, derived `euro_default_season()` | R-pure |
| X-03 | Season date bounds per season (1 Sep - 1 Jul, `season_date_bounds_for_year`); EuroLeague `euro_season_date_bounds` | R-pure (note D22: React/Plumber used 1 Oct) |
| X-04 | Reference lists: teams (distinct / min), players basic, GN values, seasons, EL phases, EL rounds -- one cache key per dataset per season | R-pure (`fetch_teams_distinct/min`, `fetch_gn_values`, `fetch_players_basic`, `euro_fetch_*`) |
| X-05 | Game-context filters, shared contract: dates, game type (IL codes, `GAME_TYPE_CHOICES_UI`, Winner Cup 10+34 one choice) / phase (EL text), opponents, home/away, outcome, opponent rank (side top/bottom, N 1-12, metric off/def/net), own/opp starters (gte/lte + 0-5), GN min/max (IL) / round (EL), last N (mutually exclusive with GN) | R-pure (`game_context_filter_values`, `game_context_db_args`, `resolve_gn_last_n_params`, `resolve_starters_bounds`) |
| X-06 | Clutch filter: enabled, max margin, score status (all/leading/trailing/tied), max minutes remaining, OT margin toggle; pre-shot margin; OT bypass | R-pure (`resolve_clutch_params`) + SQL |
| X-07 | Fast path vs filtered path: season MV when nothing narrows the games, stored function otherwise; a full-season date window counts as "no date filter" | R-pure per tab (`onoff_fallback_needed`, `clutch_reader_kind`) + R-closure (Tab 3, 5, 9 tests) |
| X-08 | Data version: `app_meta.etl_full_last_success` (IL) / `euro_data_version()` (EL) keys every season cache; caches never cross leagues | R-pure |
| X-09 | Stat filter chips ("metric >= / <= value"), per view, per tab; view switch drops (Tab 1/8, with a notice) or clears (2, 3, 4, 10) them | R-pure (`apply_stat_filters`, `retain_stat_filters_for_cols`) |
| X-10 | Filter chips bar: every active filter as a chip, each clearable, Reset to defaults | R-pure (`build_filter_chips`, `setup_chip_clears`) -- presentation |
| X-11 | Percentile ranks computed on the full population before display narrowing (team, player, min poss, stat filters) | R-pure / R-closure, per tab |
| X-12 | Heat colours from percentiles with per-metric polarity (TOV and defence inverted) | Client (from `pr_*` columns) |
| X-13 | Default sort per table and "best direction" sort sequences; NA sorted last | Client (DT options built in R) |
| X-14 | CSV export (visible columns, applied order) with timestamped file names | Client (`csv_export_button`, `csv_export_stamp`) |
| X-15 | Column reorder persisted per table (Tab 3 Traditional, Tab 5) | Client (`localStorage`) |
| X-16 | Column header tooltips / definitions (`COLUMN_TOOLTIPS`, `HEADER_TOOLTIP_JS`), glossary modal, tab explainers, table hints | R-pure constants |
| X-17 | Row pivots from On/Off rows: "Lineups with this player", "Lineups for this team", "Game log for this team", per league | Client (`app.js` pivot menu) + R (`consume_pending_nav`) |
| X-18 | Shareable state: URL bookmark of every input, restore on load (`request_restore_context`) | R + Client |
| X-19 | "Last updated" stamp in the navbar | R |
| X-20 | Request guardrails: per-session rate limit per query key (`guard_heavy_request`, 20-50 calls / 60 s), 20 s statement timeout, parameterised SQL, untrusted inputs | R-pure + DB |
| X-21 | Player identity: canonical display names, aliases (`player_identity_aliases_v`, `resolved_player_identity_v`) | SQL |

## ON. On/Off Impact -- Tab 1 (IL) and Tab 8 (EL)

| ID | Feature | Where it runs |
|---|---|---|
| ON-01 | Views: Summary, Four Factors, Shot Profile (IL only) | -- |
| ON-02 | Filters: X-05 (no clutch), teams multi-select | -- |
| ON-03 | Fast path `onoff_default_mv` + `player_advanced_stats_mv`; filtered `onoff_compute` (IL 23 params; EL 22) and `four_factors_dashboard_compute` (IL 20; EL 19) | SQL |
| ON-04 | Team filter and both possession bars applied locally after ranking (`onoff_filter_summary_rows`, `onoff_filter_ff_rows`) | R-pure |
| ON-05 | Min ALL poss / min ON poss bars, defaults 100 / 300 | -- |
| ON-06 | **Auto min possessions**: top 35% by ON poss (`AUTO_TOP_PCT`); lowers the bar, raises only to undo its own lowering, capped at the pre-auto baseline; re-arms on season, filters and view; manual slider turns it off; Reset clears the baseline | R-pure (`setup_onoff_auto_min`, `auto_min_on_from_df`, `auto_min_all_from_df`, `onoff_auto_min_base_df`) -- partly reactive |
| ON-07 | Summary: Net/Off/Def diff, on-court and off-court Off/Def PPP and Net, Off/Def shot cells (2PT/3PT accuracy + frequency, league averages from rows with >= 50 FGA, muted below 50), Min, ON/OFF poss; default sort Net desc; percentile colours | R-pure (`onoff_summary_datatable`) |
| ON-08 | **3PT luck**: league 3P% from the season frame (not the filtered rows), per-row luck ours/theirs, tag when removing it flips the sign; hover tooltip | R-pure (`onoff_league_3p`, `onoff_3pt_luck`) + Client |
| ON-09 | **Small sample**: padded net (`ONOFF_PAD_POSS` 4000), shrink, tag top/bottom 20% with shrink < 0.6x median (min 10 rows); hover | R-pure (`onoff_padded_net`, `onoff_sample_flag`) |
| ON-10 | Four Factors: eFG/OREB/TOV/FTR diffs per side + Off/Def/Net rating diffs; on/off range cells with percentile dots; ranks on full population (`onoff_add_ff_ranks`); `est. +/-X pts` impact (IL only, `FF_IMPACT_WEIGHTS`); FF ranges toggle | R-pure (`onoff_four_factors_datatable`) |
| ON-11 | Shot Profile (IL): eFG, lay-up, dunk, lay+dunk, 3PA, corner-3 share (of known-location 3PA, "--" when unknown), 2PT jumper; ON-OFF diff, ranks gated at 50 ON-side FGA; 2PT-jumper polarity flipped; default sort Off Lay+Dunk desc | **R-closure** (`sp_ranked_df` in `server_tab1.R`) |
| ON-12 | Player Stats filter chips (`ps_*`): filter on/off rows by a player's traditional stats for the same game context minus starters; memo per context; error row; starters note | R-pure (`fetch_player_stat_filter_frame`, `apply_player_stat_filters`) |
| ON-13 | Row pivots (X-17) carry the league's own targets | R-pure + Client |
| ON-14 | Luck / small-sample explainer popover | R-pure |

## LD. Lineups -- Tab 2 (IL) and Tab 10 (EL)

| ID | Feature | Where it runs |
|---|---|---|
| LD-01 | Views Summary / Four Factors; group size 2-5 (default 5) | -- |
| LD-02 | Filters: X-05 + X-06 (clutch); team single ("All teams") | -- |
| LD-03 | Player chip roster: On (all of), Any of (with "at least / exactly k of", k up to n-1), Off; roster ordered by season minutes, top 12 + more; any-of adds a clause | R-pure (`lineup_on_predicate`, `parse_any_min/exact`, `apply_local_lineup_filters`, `with_season_minutes`) + Client chips |
| LD-04 | Fetch every lineup at min poss 0, rank the full population, then narrow by team / players / min poss locally | R-closure (IL `ld_*_ranked_df`) / R-closure (EL `euro_ld_full`) |
| LD-05 | IL readers `fetch_lineups_csv_v2` / `fetch_lineups_four_factors_csv` (29 params); EL fast path `euroleague.sub_lineups_stats_mv`, filtered `fetch_lineups_pergame` / `_dynamic` / `_direct` routed by `clutch_reader_kind` | SQL |
| LD-06 | EL: every rate derived in R from summed counts (`add_rates`), never stored ratios; `to_tab2_contract` | **R-closure** |
| LD-07 | Ranks: `adaptive_baseline(total_poss)` qualification; Summary net / off / def(inv); FF ppp/efg/oreb/tov(inv)/ftr per side + net | R-closure (IL inline `pr_vec`; EL `add_pct_ranks`) |
| LD-08 | Derived total_poss, plus_minus; EL num_starters = possession-weighted mean | R-closure |
| LD-09 | **Auto min possessions**: target 150 rows, step 10, raises and lowers; computed on the team/player-filtered population before the min filter; manual -> off; any filter change -> on; Reset -> default 20 (IL) | R-pure (`auto_minposs_from_df`) + reactive glue |
| LD-10 | TOTAL row pinned at top: rates from summed raw counts; "- All Lineups -" | R-pure (`lineup_summary_datatable`, `lineup_ff_datatable`) |
| LD-11 | Summary shot cells (2PT/3PT), # starters shown as whole number; page sizes 25-1000 | R-pure |
| LD-12 | Lineup click -> game log modal: sub-lineup -> lineup hashes; per-game Off/Def PPP, net, shot cells (league avg from the log, muted < 10 FGA), poss, minutes, pts, +/-; FF variant (TS, eFG, OREB, TOV, FTR); sorted by GN | **R-closure + inline SQL** (IL `server_tab2.R`, EL `server_tab10`) |
| LD-13 | Pivot-in from On/Off / Home with team (and player) preselected | R-pure (`consume_pending_nav`) |
| LD-14 | EL chips show "N-player units / N shown / min X poss (auto)" | R-closure |

## TR. Team Ratings -- Tab 3 (IL) and Tab 9 (EL)

| ID | Feature | Where it runs |
|---|---|---|
| TR-01 | Views: Summary, Four Factors, Shot Profile (IL), Traditional (IL) with side Offense/Defense(opponents') and display Per Game / Per 75 Poss / Per 40 Min | -- |
| TR-02 | Filters: X-05 + X-06; IL has no team filter, EL has teams multi | -- |
| TR-03 | IL fast path `team_ppp_ratings_mv` / `team_four_factors_mv`; filtered `get_team_ratings_dynamic`, `get_team_four_factors_dynamic`, `get_team_four_factors_dashboard_dynamic` (standard clutch), `get_team_traditional_dynamic` (side), `get_team_minutes_dynamic` (pace) | SQL |
| TR-04 | EL readers per `clutch_reader_kind`: pergame / dynamic (dashboard incl. minutes) / direct, for ratings, FF and minutes | SQL + R-closure routing |
| TR-05 | Pace from measured minutes (`add_team_pace_cols`; EL `fallback_to_regulation = FALSE`) | R-pure |
| TR-06 | Summary: Season, Team, GP, Min, W, L, Off/Def PPP, Net, Off/Def Pace, Off/Def Poss; value + `#rank` cells, coloured by rank | R-pure (`fmt_rank_cell`) + R-closure |
| TR-07 | **Rank-change arrows** vs the previous league game day: only with season dates and no GN/last-N; re-runs the same reader with that end date in every view; IL anchor = latest game date matching filters (inline SQL) | **R-closure + inline SQL** (IL); R-pure `team_rating_rank_deltas` (EL) |
| TR-08 | Four Factors with min_rank per metric and polarity, `pr_*` colours, NA-aware sort keys, default sort net desc | R-closure (IL) / R-pure `team_rating_sort_columns` (EL) |
| TR-09 | Shot Profile (IL): shares + eFG% joined from FF; `#rank` labels (def eFG ascending); 2PT-jumper polarity flip; default sort lay+dunk desc | **R-closure** |
| TR-10 | Traditional (IL): 23 stats with rank + percentile, polarity flips on defence, per-mode transform (`apply_tr_trad_mode`) | **R-closure** |
| TR-11 | CSV per view (Traditional: team/opponent + display in the file name) | Client |

## GL. Game Logs -- Tab 4 (IL) and Tab 11 (EL)

| ID | Feature | Where it runs |
|---|---|---|
| GL-01 | Views Summary / Four Factors; team optional (IL single, EL multi) | -- |
| GL-02 | Filters: dates, game type / phase, opponents, home/away, outcome, starters own/opp, GN/round min-max, last N (each team's last N whole-season games, applied before the others). No opponent rank, no clutch | **R-closure** (all filtering in R over season pulls) |
| GL-03 | Data: IL `final_schedule_mv` + `mv_lineup_totals_by_day` + `lineup_four_factors_by_game` season pulls; EL `team_four_factors_by_game` x `final_schedule_mv` + minutes (inline SQL) | SQL / inline SQL |
| GL-04 | Per team-game metrics: Summary PPP, net, minutes, shot split metrics + shot cells (avg from shown rows); FF TS/eFG/OREB/TOV/FTR | R-pure (`gl_build_summary_metrics`, `gl_build_ff_metrics`, `add_shot_split_metrics`) |
| GL-05 | Percentiles against the whole season population, so a game keeps its colour under any filter | R-pure (`gl_attach_percentiles`) |
| GL-06 | Gameflow link per row when the game has scores (`attach_has_scores`, `fetch_scoreless_games`) | R-pure |
| GL-07 | Default sort date desc then GN/round desc (numeric date key); game-id tiebreak | Client + R |
| GL-08 | CSV per view | Client |

## PS. Player Stats -- Tab 5 (one tab, both leagues)

| ID | Feature | Where it runs |
|---|---|---|
| PS-01 | Display modes: Totals, Per Game (default), Per 60 Possessions, Per 30 Minutes (`apply_ts_mode`, x_poss / x_min = 75th percentile of eligible per-game poss/min, eligible = gp >= 3) | R-pure + R-closure context |
| PS-02 | Rate-mode eligibility: possession floor = 15th percentile when >= 120 players (`ts_rate_threshold`); hidden unless "show ineligible" (then greyed); banner with counts; base column Min/G or Poss/G | **R-closure** (functions top-level in the server file) |
| PS-03 | Filters: dates, teams multi, players multi (`<team>:<player>`, canonical names), X-05, X-06, Min GP slider + number (max = population max GP), stat filters (>= / <= on 28 columns incl. USG%; Total Poss hidden in Totals; DFL absent in EL), show ineligible | -- |
| PS-04 | Fast path `player_traditional_stats_mv` season pull (shared cache); filtered `fetch_player_traditional_filtered` (IL `_from_games` / `_custom_clutch`; EL readers) | R-pure + SQL |
| PS-05 | Derived 2PM/2PA/2P%, USG% (`add_ts_usage_pct`), row cleanup | R-pure / top-level in server file |
| PS-06 | **Multi-team TOTAL rows** per identity: summed counts, recomputed %, TS from trips (`ts_attempts`), possession-weighted USG; grouped order (group at max PTS, TOTAL after its parts); bold + tint; selecting any component keeps the TOTAL | top-level in server file (`add_ts_multi_team_totals`, `ts_group_display_order`, `filter_ts_players`) |
| PS-07 | Percentiles over the full population before player / Min GP / stat narrowing; rows below `adaptive_baseline(poss)` unranked; TOV reversed | top-level in server file (`add_ts_percentile_cols`) |
| PS-08 | Default order PTS desc, minutes desc, team, player; column reorder persisted per league; CSV per league | Client |
| PS-09 | League switch clears teams, players, opponents, H/A, outcome, rank, GN | R-closure |

## CMP. Compare -- Tab 7 (Israeli only)

| ID | Feature | Where it runs |
|---|---|---|
| CMP-01 | Modes Teams / Lineups / Players; switching mode resets all filters | R-closure |
| CMP-02 | Presets: Starters vs Bench (own >= 3 / <= 2), Vs Starters vs Vs Bench (opp), Clutch vs Non-Clutch, Home vs Away, Win vs Loss, Top vs Bottom (opp rank top 4 / bottom 4, net), Date split (A <= date < B), GN split (A <= GN < B) | **R-closure** |
| CMP-03 | Preset side labels (Starters/Bench, vs Top 4, Before Oct 03, GN 1-12 ...); full side descriptions | R-closure |
| CMP-04 | Gap direction B-A for date/GN split, else A-B; winner by metric polarity (`TEAM_RATING_METRICS`) | R-closure |
| CMP-05 | Side mirroring: with a preset active, a non-defining field edited on one side is copied to the other | R-closure (UI behaviour) |
| CMP-06 | Per-side filters: starters own/opp, teams, H/A, outcome, clutch (margin 0-10, minutes 1-5), opponents, game type, opp rank; shared dates + GN | R-closure (`collect_side_params`) |
| CMP-07 | **Min possessions with auto**: target 50 rows, step 10, on min(poss A, poss B) of the unfiltered join; bootstraps on first run; manual -> off; filter change -> on; Reset -> 10 | R-closure |
| CMP-08 | Metric chips Net / Offense / Defense / eFG% / TOV% / OREB% / FTR | -- |
| CMP-09 | Teams: per-side ratings / FF readers (with `team_ratings_preset_cache` fast path for preset-shaped requests), side team filter, join, gap, rank, sort gap desc NA last | R-closure + SQL |
| CMP-10 | Lineups: size 2-5, inline lineup filter (team, all-of, any-of re-applied in R, off), join on sub-lineup, top 50 by gap | R-closure + SQL |
| CMP-11 | Summary cards: A / B value (Lineups poss-weighted), delta, mean poss, league avg abs gap, winner | R-closure |
| CMP-12 | Stat filter chips on A / Poss A / B / Poss B / Gap | R-pure |
| CMP-13 | Detail view per team or lineup: Ratings (Win% teams only), Off FF, Def FF (est. pts), Off shooting, Def shooting + Off/Def shot profile (teams only); context bar; gap bars; client gap sort | **R-closure** |
| CMP-14 | Teams detail -> Players: per-player A vs B with metric chips (12) and rate (Per Game / Per 60 Poss / Per 30 Min / Totals), "didn't play", min poss on max side; player drill-down (Usage, Production, Shooting) | **R-closure** |
| CMP-15 | Players mode: "Other players" (shared dates/GN) or "Against himself" (A/B dates + GN); per-side list team filter; optional team when a player has several; default players = 2 random of the top-3 PPG, cleared once filters change | **R-closure** |
| CMP-16 | Players views: Overall (11 stats, Per Game / Per 75 Poss / Totals, GP, MPG); Four Factors swing (diffs + est. pts, disruptions/100 when both sides >= 300 def poss); Shot Profile swing | **R-closure** |
| CMP-17 | Pending preset from Home storylines: preset + team pinned on both sides + open that team's detail | R-closure |

## HOME. Home hub (Israeli section)

| ID | Feature | Where it runs |
|---|---|---|
| HOME-01 | Team selector; remembered default team (browser storage), "set as default", fallback team | R-closure + Client |
| HOME-02 | One combined dashboard query with validation and per-section fallbacks | R-pure (`hub_fetch_dashboard`) |
| HOME-03 | Identity card: record, Off/Def/Net with rank "Nth of N", FF mini chips with ranks | R-pure (`hub_identity_data`, `hub_ff_mini`) |
| HOME-04 | Last game + Gameflow link when available | R-pure (`hub_fetch_last_games`, `hub_last_game`) |
| HOME-05 | Key players (top on/off net, >= 100 ON poss) + top scorer (min 3 GP) | R-pure |
| HOME-06 | Best / worst 5-man lineup (>= 100 poss) | R-pure |
| HOME-07 | Storylines (starters vs bench, clutch, last 10, vs top 4 / bottom 4) with deep links into Compare | R-pure (`hub_storyline_specs/lines`) |
| HOME-08 | Question cards and deep links into tabs | UI |

## GF. Gameflow (stint ribbon) -- Game Logs (IL, EL) and Home

| ID | Feature | Where it runs |
|---|---|---|
| GF-01 | Per game and team: player lanes with on-floor stints, period bounds, running margin, starters marked, lineup dictionary | inline SQL in `global.R` (`RIBBON_SQL_ISRAEL`, EL equivalent) + R-pure transforms |
| GF-02 | Per-player and per-window totals (minutes, +/-), window detail, period detail | R-pure (`ribbon_*`) |
| GF-03 | Health warnings (strict-inside straddles; game 406 Q4 disclosure) | R-pure (`ribbon_health_message`, `ribbon_game_warning`) |
| GF-04 | SVG rendering, hover, pair selection; touch quarter cards | R-pure SVG + Client |
| GF-05 | Both teams in one view (own lanes, margin chart, opponent lanes); per-stint +/- labels; per-player MIN and +/- | R-pure (seen in the live app) |
| GF-06 | Select two or more players: shared court time ("8:45 on court together"), stints re-split at the shared windows, margin chart highlighted, "Shared lineups" list (lineup, shared time, +/-, clock windows), select a lineup to highlight its minutes, Clear players | **Client** (`app.js`, computed from the SVG's data attributes) -- the API must return per-stint lineup, clock and score data so a client can recompute it |

## HOME (continued)

| ID | Feature | Where it runs |
|---|---|---|
| HOME-09 | League chooser cards (EuroLeague / EuroCup / Israeli League) on Home; EuroLeague Home shows question cards only, no hub | UI |

## Counts

| Section | Features |
|---|---|
| Cross-cutting | 21 |
| On/Off | 14 |
| Lineups | 14 |
| Team Ratings | 11 |
| Game Logs | 8 |
| Player Stats | 9 |
| Compare | 17 |
| Home | 8 |
| Gameflow | 6 |
| **Total** | **109** |

(Home now counts 9 with HOME-09.)

## Checked in the live app (2026-10-19)

Every tab was opened in the production app and, where production was unreliable, in a
local copy of `main` at `c6dc605`. The visible controls of each tab (Israeli and
EuroLeague) were listed from the page and compared with this inventory; they match,
except for the items added above (GF-05, GF-06, HOME-09). What the walk-through also
found -- features the screen offers that do not work:

| Finding | Evidence | Effect |
|---|---|---|
| **On/Off "Download CSV" is a dead button.** `ui_tab1_onoff.R:43` renders `downloadButton("download_csv")`; no server code defines `output$download_csv`, so the button stays disabled | live page: class `shiny-download-link disabled`; grep finds no handler | On/Off cannot be exported, unlike every other tab |
| **Compare Teams detail view is blank under the two starters presets.** Starters vs Bench and Vs Starters vs Vs Bench: the detail calls `get_team_four_factors_dynamic` per side, which runs past the 20 s statement timeout | production page: `shiny-output-error`; local log: `canceling statement due to statement timeout` (twice, 20.3-20.5 s); Home/Away, Clutch, Win/Loss and Top/Bottom presets render | A user sees an empty panel with no message |
| Compare detail subtitle shows the wrong season ("2027-28" for 2026-27): it treats the season-ending year as the start year | local page text | Label only |
| Compare: the Clutch preset's full side label reads "Clutch vs Side B", not "Non-Clutch" | local page text | Label only |
| Home: a team with one qualifying lineup shows it as both Best and Worst | live page (Bnei Herzliya) | Label only |
| Production session dropped and reloaded once during the walk-through and came back disconnected (20 console errors) | live page | Known cold-start issue (CLAUDE.md, Connect Cloud console errors) |

The API inventory treats the first two as features to deliver working (CSV of the
On/Off table; detail view under every preset), not as behaviour to copy.

## What this means for the API

1. **Most behaviour is not in SQL.** Rankings, tags, TOTAL rows, auto min-possessions,
   rate modes and all of Compare run in R; much of it inside Shiny closures (marked
   R-closure). An API that only wraps the stored functions would drop about half the
   features above.
2. **Five places still query in inline SQL from R**: the lineup game-log modal (IL and
   EL), the Tab 3 rank-arrow anchor, the EL game-log pull, the Gameflow readers. Each
   needs one owner the API and Shiny both call (rule R1).
3. **Presentation features (X-10, X-12 to X-16, GF-04) are client work.** The API's job
   is to return what a client needs to reproduce them: `pr_*` percentiles with polarity,
   default sort and best-direction per column, column definitions and labels, CSV-ready
   rows.
