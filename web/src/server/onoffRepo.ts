import "server-only";
import { withSnapshot, Deadline, type Query } from "./db";
import type { Row } from "../rules/gameLogs";
import { dbArgs, needsFilteredPath, type Context } from "./contextParams";
import { readVersion } from "./gameLogsRepo";
import { VersionedCache } from "./versionedCache";
import { applyPlayerNames, readPlayerNames } from "./playerNames";
import {
  addFfRanks,
  cleanDisplayNames,
  joinFfRatings,
  league3p,
  shotProfileRanked,
  type View,
} from "../rules/onoff";

// The readers Tab 1 (server_tab1.R) and Tab 8 (server_tab8_euro.R) call,
// with their argument lists unchanged.
const SQL = {
  ilMv: 'SELECT * FROM basketball_test.onoff_default_mv WHERE "Year" = $1::int4 ORDER BY "Net RTG Diff" DESC, "Team", "Last Name", "First Name"',
  ilAdv:
    "SELECT * FROM basketball_test.player_advanced_stats_mv WHERE game_year = $1::int4",
  ilCompute:
    "SELECT * FROM basketball_test.onoff_compute($1::date,$2::date,$3::text,$4::int4,$5::int4,$6::numeric,$7::text,$8::text,$9::text,$10::text,$11::text,$12::text,$13::int4,$14::text,$15::int4,$16::int4,$17::int4,$18::int4,$19::int4,$20::int4,$21::int4,$22::int4,$23::int4)",
  ilFf: "SELECT * FROM basketball_test.four_factors_dashboard_compute($1::int4,$2::date,$3::date,$4::text,$5::text,$6::text,$7::text,$8::text,$9::text,$10::int4,$11::text,$12::int4,$13::int4,$14::int4,$15::int4,$16::int4,$17::int4,$18::int4,$19::int4,$20::int4)",
  elMv: 'SELECT * FROM euroleague.player_onoff_default_mv WHERE competition = $1::text AND game_year = $2::int4 ORDER BY "Net RTG Diff" DESC, "Team", "Last Name", "First Name"',
  elAdv:
    "SELECT * FROM euroleague.player_advanced_stats_mv WHERE competition = $1::text AND game_year = $2::int4",
  elCompute:
    "SELECT * FROM euroleague.onoff_compute($1::text,$2::int4,$3::date,$4::date,$5::text,$6::text,$7::text,$8::text,$9::text,$10::text,$11::int4,$12::text,$13::int4,$14::int4,$15::int4,$16::int4,$17::int4,$18::int4,$19::int4,$20::numeric,$21::int4,$22::int4)",
  elFf: "SELECT * FROM euroleague.four_factors_dashboard_compute($1::text,$2::int4,$3::date,$4::date,$5::text,$6::text,$7::text,$8::text,$9::text,$10::text,$11::int4,$12::text,$13::int4,$14::int4,$15::int4,$16::int4,$17::int4,$18::int4,$19::int4)",
  // Reference lookups: fetch_gn_values(), euro_fetch_teams(), euro_fetch_round_values(),
  // euro_fetch_phases(), euro_fetch_seasons().
  ilGn: "SELECT DISTINCT gn FROM basketball_test.final_schedule_mv WHERE game_year = $1::int4 ORDER BY gn",
  elTeams:
    "SELECT team_id, MIN(team_name) AS team_name FROM euroleague.final_schedule_mv WHERE competition = $1::text AND game_year = $2::int4 GROUP BY team_id ORDER BY MIN(team_name)",
  elRounds:
    "SELECT DISTINCT round_number AS gn FROM euroleague.final_schedule_mv WHERE competition = $1::text AND game_year = $2::int4 AND round_number IS NOT NULL ORDER BY round_number",
  elPhases:
    "SELECT DISTINCT phase FROM euroleague.final_schedule_mv WHERE competition = $1::text AND game_year = $2::int4 AND phase IS NOT NULL ORDER BY phase",
  elSeasons:
    "SELECT DISTINCT game_year FROM euroleague.final_schedule_mv WHERE competition = $1::text ORDER BY game_year DESC",
};
const DEFAULT_MIN_NET = -1e9; // global.R

export type OnOffAnswer = {
  rows: Row[];
  league3p: number | null;
  filtered: boolean;
  version: string;
  teams: { id: number; name: string }[];
  rounds: number[];
  phases: string[];
  seasons?: string[];
};

async function read(q: Query, c: Context, view: View): Promise<OnOffAnswer> {
  const euro = c.league !== "il";
  const comp = c.league === "eurocup" ? "U" : "E";
  const a = dbArgs(c);
  const filtered = needsFilteredPath(c);
  const version = await readVersion(q, c.league);
  // The season view: the fast path, and the season frame onoff_league_3p() reads.
  const mv = await q<Row>(
    euro ? SQL.elMv : SQL.ilMv,
    euro ? [comp, c.season] : [c.season],
  );
  const compute = (teams: number[]) => {
    const teamCsv = teams.length ? teams.join(",") : null;
    return euro
      ? q<Row>(SQL.elCompute, [
          comp,
          c.season,
          c.start,
          c.end,
          teamCsv,
          a.typeCsv,
          a.oppCsv,
          a.homeAway,
          a.outcome,
          a.rankSide,
          a.rankN,
          a.rankMetric,
          a.minGn,
          a.maxGn,
          a.lastN,
          a.offMin,
          a.offMax,
          a.defMin,
          a.defMax,
          DEFAULT_MIN_NET,
          0,
          0,
        ])
      : q<Row>(SQL.ilCompute, [
          c.start,
          c.end,
          teamCsv,
          0,
          0,
          DEFAULT_MIN_NET,
          String(c.season),
          a.typeCsv,
          a.oppCsv,
          a.homeAway,
          a.outcome,
          a.rankSide,
          a.rankN,
          a.rankMetric,
          a.minGn,
          a.maxGn,
          a.lastN,
          null,
          null,
          a.offMin,
          a.offMax,
          a.defMin,
          a.defMax,
        ]);
  };
  let rows: Row[];
  if (view === "four_factors") {
    rows = filtered
      ? await (euro
          ? q<Row>(SQL.elFf, [
              comp,
              c.season,
              c.start,
              c.end,
              null,
              a.typeCsv,
              a.oppCsv,
              a.homeAway,
              a.outcome,
              a.rankSide,
              a.rankN,
              a.rankMetric,
              a.minGn,
              a.maxGn,
              a.lastN,
              a.offMin,
              a.offMax,
              a.defMin,
              a.defMax,
            ])
          : q<Row>(SQL.ilFf, [
              c.season,
              c.start,
              c.end,
              null,
              a.typeCsv,
              a.oppCsv,
              a.homeAway,
              a.outcome,
              a.rankSide,
              a.rankN,
              a.rankMetric,
              a.minGn,
              a.maxGn,
              a.lastN,
              null,
              null,
              a.offMin,
              a.offMax,
              a.defMin,
              a.defMax,
            ]))
      : joinFfRatings(
          await q<Row>(
            euro ? SQL.elAdv : SQL.ilAdv,
            euro ? [comp, c.season] : [c.season],
          ),
          mv,
        );
    rows = addFfRanks(rows);
  } else if (view === "shot_profile") {
    // Shot Profile ranks over the whole population: no team in SQL.
    rows = shotProfileRanked(filtered ? await compute([]) : mv);
  } else {
    // Summary's filtered path filters teams in SQL (server_tab1.R
    // live_unfiltered_df); the fast path filters them on the page.
    rows = filtered ? await compute(c.teams) : mv;
  }
  const meta = await readTabReferences(q, c);
  return {
    rows: cleanDisplayNames(
      applyPlayerNames(rows, await readPlayerNames(q, c.league, c.season)),
    ),
    league3p: league3p(mv),
    filtered,
    version,
    ...meta,
  };
}

/** The existing Shiny reference readers, shared by the migrated tabs. */
export async function readTabReferences(q: Query, c: Context) {
  const euro = c.league !== "il";
  const comp = c.league === "eurocup" ? "U" : "E";
  return euro
    ? {
        teams: (
          await q<{ team_id: number; team_name: string }>(SQL.elTeams, [
            comp,
            c.season,
          ])
        ).map((t) => ({ id: Number(t.team_id), name: t.team_name })),
        rounds: (await q<{ gn: number }>(SQL.elRounds, [comp, c.season])).map(
          (r) => Number(r.gn),
        ),
        phases: (
          await q<{ phase: string }>(SQL.elPhases, [comp, c.season])
        ).map((r) => r.phase),
        seasons: (await q<{ game_year: number }>(SQL.elSeasons, [comp])).map(
          (r) => String(r.game_year),
        ),
      }
    : {
        teams: [],
        rounds: (await q<{ gn: number }>(SQL.ilGn, [c.season]))
          .map((r) => Number(r.gn))
          .filter(Number.isFinite),
        phases: [],
      };
}

const cache = new VersionedCache<OnOffAnswer>(16, (value) => value.version);
/** Bounded warm-instance answers, isolated by current publication version. */
export function getOnOff(c: Context, view: View): Promise<OnOffAnswer> {
  const filtered = needsFilteredPath(c);
  const key = JSON.stringify({
    ...c,
    view,
    teams: filtered && view === "summary" ? c.teams : [],
  });
  return cache.get(c.league, key, () =>
    withSnapshot(new Deadline(25000, 20000), (q) => read(q, c, view)),
  );
}
