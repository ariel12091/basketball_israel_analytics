# Gameflow: runs with an estimated cause, and ratings on the player card

Date: 2026-11-10. Surface: the web app (`web/`, Vercel). Shiny is not changed.

## Why

The user wants the app to hand them the answer to "what was the defining
stretch, and why did they lose?" without digging through tables. Their own
example: Maccabi 86-75 Hapoel Tel Aviv, 2026-11-09 (Winner Cup, `game_id` 673).
The game turned at Q3 3:54, when Maccabi changed its lineup.

The data finds that moment on its own (checked 2026-11-10, from
`df_pts_poss_lineups_longer_mv`). Maccabi's five of Clark III, DiBartolomeo,
Leaf, Theis and Wallace played Q3 3:54 to Q4 4:54 without a change and won it
29-5, taking the score from 36-59 to 65-64. The next best stint in the game was
+5. Inside that stretch Maccabi went 6/10 from three (6/19 the rest of the game)
and Hapoel turned it over 7 times in 16 possessions (9 in 58 otherwise). Before
it, Hapoel had a run of 18-0 while Maccabi went 0/7 from the field across five
lineups. Hapoel also substituted at Q3 3:54 (Blakeney and Timor in, Bryant and
Carrington out), so the lineup change is a timing match, not a proven cause.

This design is the first of the "actionable insights" the user asked for. The
next one, "this player is better with that player", is a separate design to
follow; it is out of scope here.

## Goals

1. **Runs are marked on Gameflow** (user chose mockup A). Each run is a shaded
   band across the margin chart and the player rows below, in the colour of the
   team that made it. A chip above the band names it: team short name and the
   run's score, e.g. `MTA 29-5 ›`. One run per game is the **defining stretch**,
   the largest swing in net points. It gets the strong colour and the filled
   chip; other runs get a lighter band and an outlined chip.
2. **Each run explains itself in a tooltip.** It opens on hover of the band or
   chip on pointer devices, and on a tap of the chip on touch. It has four
   parts, in this order (user: lineup first):
   1. The window and the swing: "Q3 3:54 – Q4 4:54 · from down 23 to up 1".
   2. **Lineup.** Who came in and who went out just before the run started or
      during it, for **both** teams, and whether the run team's five stayed on
      for the whole run. Both teams' changes are always shown, so the swing is
      never pinned on one coach when both moved. When neither team changed,
      the line says so ("same five as before the run") and the results carry
      the explanation.
   3. **Results.** At most two numbers that stand out most against **the rest
      of the same game** (user), from shooting (2PT, 3PT, FT), turnovers,
      offensive rebounds and rating. Example: "6/10 from three (6/19 the rest
      of the game)", "Hapoel 7 turnovers in 16 possessions (16% otherwise)".
   4. An "Estimated cause" line, worded as a timing match, never as proof.
3. **Small samples are handled honestly** (user: four factors and rates swing
   a lot on small samples):
   - A number appears in the tooltip only when it is far from the rest of the
     game **and** rests on enough attempts.
   - Counts are always shown with the rate ("6/10"), never a bare percentage.
   - A hot or cold three-point stretch is labelled shooting variance and is
     never credited to the lineup (the user's model, already used by the On/Off
     "3PT luck" tag: 3P% is luck, 2P% is not). Turnovers, rebounds and 2PT are
     not given that label.
4. **Ratings on the player card.** The card (one player's game, a clicked
   stint, "Together" for selected teammates) keeps everything it shows today
   (on the floor, +/-, points for and against) and **adds Net rating, ORtg and
   DRtg**, per 100 possessions, each with its possession count ("Net +31.4 on
   14 poss").
5. **Net rating in the hover tooltip** of a bar, after the +/-:
   "6:12 on the floor · +/- +8 · Net +31.4 on 14 poss".
6. **Both leagues** (user: parity). EuroLeague Gameflow gets possession ends,
   the ratings and the runs, the same as the Israeli league.

## Non-goals

- Player-pair insights ("better with X"): the next design.
- Saying "small lineup" or naming positions. The data has no positions or
  heights that this work may rely on; the tooltip names players and changes.
- Changing the +/- or the points for/against on the card or the bars. They
  keep following the floor (see Constraints).
- Shiny. It retires at the Vercel cut-over.
- Aligning the EuroLeague free-throw credit rule. It is a planned follow-up
  (see Follow-up), not part of this work.

## Anti-goals (would make this a failure even if it works)

- A tooltip that blames a lineup for what was a hot shooting streak, or that
  credits one team's substitution when both changed.
- A number quoted from 2 attempts. A rate shown without its count.
- Ratings on the card that disagree with the On/Off tab for the same player and
  game, beyond the documented EuroLeague credit difference.
- A second implementation of a rating or a possession count (rule 1, rule 4 of
  `docs/shared_definitions.md`).

## Constraints

- **`docs/shared_definitions.md` is the contract.** Before any code, add the
  new terms to it: "Gameflow run" (detection and the defining stretch),
  "Gameflow stint rating" (Net, ORtg, DRtg on a card or bar), and the
  "stands out" rule for tooltip results. Each with its owner. Update the
  existing "Gameflow ORtg" row, which says EuroLeague possession ends are not
  in its curated read layer yet.
- **Ratings follow the On/Off convention** (user): events by credited lineup,
  so a free throw counts for the lineup that drew the foul. Rule R6 currently
  says "Gameflow uses the physical lineup columns"; the ratings are events, so
  they follow credit, and R6's Gameflow line must be amended to say floor time,
  bars, +/- and points for/against stay physical while ratings follow credit.
  Record it the same day.
- **The +/- and points next to the ratings stay physical**, as the ribbon is
  today (the existing, documented ~3.5% of points that differ). The card's info
  text gets one sentence saying the ratings are counted the On/Off way and can
  differ slightly from the +/- beside them.
- **EuroLeague credit differs today.** EuroLeague credits a trip to the lineup
  on the floor for its last free throw (D1). So the EuroLeague card matches
  EuroLeague On/Off, and the two leagues differ on that one rule until the
  follow-up lands. Say so in the deviation register.
- **EuroLeague possession ends are not in the curated read layer yet** (per
  `shared_definitions.md`). The schema has them (possession counter, canonical
  actions), so this is a read-layer addition in `euroleague/sql/` as a numbered
  migration, followed by the security re-apply; the EuroLeague load itself
  should not change. Time any new view on a full season (memory: EL 057).
- **The data the tooltip needs is not in today's payload.** The Gameflow
  payload (`RawRibbon`, `web/src/rules/gameflow.ts`) carries lanes, score steps
  and (Israeli only) possession ends with the score, but no shots, turnovers or
  rebounds, and no credited lineup on the possession ends. The reader
  (`RIBBON_SQL_ISRAEL` in `global.R`, exported to `web/src/data/gameflowSql.json`,
  and its EuroLeague twin) must return what the runs and ratings need. Keep
  the SQL readers as the owners of the counts; `web/src/rules/` only derives
  rates and picks what to show, as `ratingSeries()` does today.
- Web rules from `web/CLAUDE.md`: work on a short `react/` branch, Preview live
  checks before merge, mobile first, React state for popovers, TabKit/existing
  Gameflow components extended rather than copied.
- Budget: Gameflow responses are cached; a larger payload costs Fast Origin
  Transfer (10 GB/month). Keep additions to compact per-event rows.

## Decided by the user

- Runs live on Gameflow; the cause is a tooltip (not a separate page or card).
- Mockup A: shaded band plus chip, over the chart and the player rows.
- Tooltip leads with the lineup; results second.
- Results are compared against the rest of the same game.
- Card: raw numbers stay, Net/ORtg/DRtg are added. Tooltip: Net added to +/-.
- Ratings follow the On/Off convention.
- EuroLeague gets possessions and everything else here, for parity.
- The EuroLeague free-throw credit fix comes afterwards.

## My calls (please check these)

- **Run definition, starting point** (user: a run need not start or end at a
  substitution or break; sometimes it is simply a run): defined by the score
  alone. A run is a stretch from one scoring event to another in which one
  team outscores the other by at least 10 net points while the other team
  scores little (e.g. 12-2 qualifies, 20-12 does not). It starts at the run
  team's first point and ends at its last, wherever those fall, and may cross
  substitutions and period breaks. "Just before" for the lineup line means a
  substitution within about a minute of game clock before the first point.
  Mark at most three runs per game (the defining stretch plus the two next
  largest), so the chart is not a field of bands. The builder tunes the
  thresholds against a handful of real games and records the final rule in
  the shared definitions.
- **"Enough attempts":** at least 6 attempts for a shooting split and at least
  8 possessions for a turnover or rating comparison, and the run's rate must
  differ from the rest of the game by a margin the builder sets per metric. If
  the rest of the game has too few attempts to compare, fall back to the team's
  season level for that game's season; if neither qualifies, show no number.
- **Ratings always carry the possession count**; short stints are not hidden.
- **Head to head** (players from both teams selected) keeps the score only, as
  the +/- does today; there is no side to rate from.
- Free-throw attribution at a run's edges follows the credit convention for
  results, and the floor for the band's start and end.

## Left to the builder

- Exact thresholds above, and the wording of each tooltip sentence.
- Colour tokens, chip geometry, band opacity; mobile chip placement in the
  quarter cards and full timeline.
- How the reader returns the extra events (one compact event list vs a
  per-stint aggregate), as long as counts come from SQL.
- Test design: pins that the card's per-game Net/ORtg/DRtg equal the On/Off
  numbers for the same player and game (both leagues), that the Israeli
  possession totals still match `team_metrics_by_game_mv`, and that game 673
  marks the 29-5 as the defining stretch.

## Follow-up (not in this work)

- Align EuroLeague's free-throw credit with the Israeli rule (D1), after which
  the leagues' card ratings mean the same thing.
- Player-pair insights: separate design.
