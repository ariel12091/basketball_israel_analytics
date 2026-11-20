# Team Ratings shooting luck (3PT + opponents' FT) -- design

Date: 2026-11-20. Phase 1 of 3 (Team Ratings; then Lineups, then On/Off).

## Intent

On/Off and Lineups tag a row "3PT luck" when its Net RTG changes sign once
three-point shooting above or below the league rate is taken out. The user
wants Team Ratings to get the same check, and wants free-throw luck added to
the check everywhere. Phase 1 builds both for Team Ratings; phases 2 and 3 add
the free-throw part to Lineups and On/Off.

## Decisions (user)

- **Free-throw luck counts the opponents' free throws only.** A defence has no
  say over the other team's FT%; a team's own FT% reflects who shoots and is
  treated as skill.
- **Approach A:** each page's own database source returns the counts the luck
  needs; the web derives the luck from them (as 3PT luck is derived today).
  Not approach B (borrowing counts from the Traditional reader with extra
  queries): it would not help On/Off or Lineups and leaves two mechanisms.
- **Team Ratings first**, one page per PR.

## Goals

- Team Ratings Summary view, both leagues (Israeli, EuroLeague, EuroCup), on
  every path the page reads (unfiltered view, filtered function, previous-end
  call for the rank arrows, EuroLeague per-game / standard-clutch / direct /
  dashboard readers), shows a luck tag on the Net cell when removing 3PT luck
  (own and opponents') plus opponents' FT luck flips the Net RTG's sign.
- The tag's explanation shows the parts separately.

## Non-goals

- Lineups and On/Off free-throw luck (phases 2-3).
- Shiny. Its Tab 3/9 read the same sources and must keep working (new columns
  are appended), but get no tag. Record this in the deviation register.
- Four Factors, Shot Profile and Traditional views (the tag is Summary-only,
  as on Lineups).
- A sample-size tag for teams.

## Anti-goals

- No re-derivation of counts in the web or in a second SQL file: the counts
  come from the readers that own Team Ratings.
- No change to any existing number (ratings, ranks, possessions, shot profile)
  -- the SQL change only appends columns. Verify existing columns are
  row-identical before and after on a full season.

## Definitions (to add to `docs/shared_definitions.md`, same day)

Per team row, with `r3` = league 3P% and `rft` = league FT%:

- `luck_3pt_ours   =  300 x (off_fg3_made - r3 x off_fg3_att) / off_poss`
- `luck_3pt_theirs = -300 x (def_fg3_made - r3 x def_fg3_att) / def_poss`
- `luck_ft_theirs  = -100 x (def_ftm      - rft x def_fta)    / def_poss`
- `net_without_luck = net_rtg - luck_3pt_ours - luck_3pt_theirs - luck_ft_theirs`
- Tagged when `net_rtg != 0` and `net_without_luck x net_rtg <= 0` (the same
  rule as On/Off and Lineups).
- League rates are the **league average** (user 2026-11-20: "luck is simply
  FT on league average rate"): every team's own shots under the same game
  filters, `r3 = sum(off_fg3_made) / sum(off_fg3_att)`,
  `rft = sum(off_ftm) / sum(off_fta)`. When a EuroLeague team filter narrows
  the rows, the rates come from the same read without the team filter, never
  from the narrowed rows.
- Free throws: all free throws, technical and flagrant included (as FTR), on
  the same predicates the team readers already use
  (`type = 'freeThrow'`, made = `parameters_made = 'made'`). 3PM:
  `type = 'shot' AND parameters_points = 3 AND parameters_made = 'made'`.
- Any missing count or a zero possession denominator gives NULL luck and no
  tag.

## Database changes

Append to every Team Ratings Summary reader's output, in both leagues:
`off_fg3_made`, `def_fg3_made`, `def_ftm`, `def_fta` (and, for symmetry and
the league rate, `off_ftm`, `off_fta`). Counts only; no rates in SQL.

Israeli:
- `basketball_test.team_ppp_ratings_mv` (`sql/materialized_views/team_ppp_ratings_mv.sql`)
  -- DROP + CREATE. Check dependents first (`get_team_ratings_dynamic` reads
  it for the opponent-rank filter; any view depending on it is dropped by
  CASCADE and must be rebuilt). Use `scripts/restage_matview.R`-style staging
  and prove existing columns equal the live view.
- `basketball_test.get_team_ratings_dynamic` (`sql/functions/get_team_ratings_dynamic.sql`)
  -- add the counts to `game_agg`, `base_agg`, `pivoted`, `final_calc` and the
  final SELECT. The return type changes, so DROP + CREATE.

EuroLeague (numbered, gated migration in `euroleague/sql/`, plus
`euroleague_function_contract.py`):
- `euroleague.team_ppp_ratings_mv`, `get_team_ratings_pergame`,
  `get_team_ratings_dynamic`, `get_team_ratings_direct`, and the dashboard
  readers `get_team_dashboard_{pergame,dynamic,direct}` as far as the Summary
  view reads them (`readFacts()` in `web/src/server/teamRatingsRepo.ts` is the
  list of what is called). Confirm each against the deployed definitions
  before editing.

After deploying: re-apply DB security (`scripts/apply_db_security.R`,
`CONFIRM_DB_SECURITY_APPLY=1`) and then curl EuroLeague pages -- a re-apply
has revoked unlisted EuroLeague grants before. Call each function once after
deploying. Time each changed function and view on a **full** season (2025-26
both leagues), and the MV refresh under the ETL's 2-minute limit.

## Web changes

- `web/src/rules/teamRatings.ts`: `teamShootingLuck(rows)` returning the rows
  with `luck_3pt_ours`, `luck_3pt_theirs`, `luck_ft_theirs`,
  `net_without_luck`, `luck_flag`; rows without the columns come back
  unchanged. Unit-tested on hand-made rows (sign flip, no flip, missing
  counts, zero possessions, league rate on full vs narrowed rows).
- `web/src/server/teamRatingsRepo.ts`: apply it to the Summary rows (current,
  not `previous`) after the rows are read.
- `web/src/components/TeamRatingsClient.tsx`: show the tag on the Net cell and
  in the table key, reusing the On/Off/Lineups tag component and `tagText`
  (add a `"shooting"` tag kind with its label "Shooting luck", explanation and
  a worked example in `web/src/data/layoutUi.json`). The explanation lists the
  three parts in points per 100.
- The API (`/api/v1/[league]/team-ratings`) returns the new fields; update
  `apiCoverage` / the API shape tests if they list columns. The Ask chat's team
  tool may surface the flag the same way it surfaces lineup luck
  (`web/src/ask/semantics.ts`) -- builder checks and follows the lineup pattern.

## Pins

- Nightly DB test: for the current season, the filtered function called with
  the full-season window returns the same six new counts as the view, per
  team (rule 10), both leagues; EuroLeague per-game vs direct readers agree.
- A check that can fail: own 3PM and FTM from the ratings readers equal
  `get_team_traditional_dynamic`'s offence `3pm` / `ftm` for the full season.

## Calls left to the builder

- Exact wording of the tag explanation and example (plain, numbers once).
- Rank arrows: `previous` rows need no luck fields.

## Later phases (for context, not built here)

- Phase 2, Lineups: add opponents' FTM/FTA to the lineup readers and per-game
  tables, extend `lineupThreePtLuck()` (and rename the tag "Shooting luck").
- Phase 3, On/Off: the same for `onoff_default_mv` / `onoff_compute` and the
  EuroLeague on/off readers, extending `threePtLuck()`.
- The R twins (`lineup_3pt_luck()`, `onoff_3pt_luck()`) are not extended:
  deviation register entry, as Shiny retires.
