import "server-only";
import { withSnapshot, Deadline } from "./db";
import type { Row } from "../rules/gameLogs";
import { playerStatProjection, type PsRow } from "../rules/playerStats";
import { dbArgs, needsFilteredPath, type Context } from "./contextParams";

// fetch_player_stat_filter_frame() in helpers.R: the season view when the
// game context (starter filters dropped -- the reader aggregates whole games)
// is the whole season, otherwise the per-game reader with no team and no
// clutch filter. Both leagues, their readers and argument lists unchanged.
const SQL = {
  ilMv: "SELECT * FROM basketball_test.player_traditional_stats_mv WHERE game_year = $1",
  ilGames:
    "SELECT * FROM basketball_test.get_player_traditional_from_games($1::int4,$2::date,$3::date,$4::text,$5::text,$6::text,$7::text,$8::text,$9::text,$10::int4,$11::text,$12::int4,$13::text,$14::int4,$15::bool,$16::int4,$17::int4,$18::int4)",
  elMv: "SELECT * FROM euroleague.player_traditional_stats_mv WHERE competition = $1::text AND game_year = $2::int4",
  elGames:
    "SELECT * FROM euroleague.get_player_traditional_pergame($1::text,$2::int4,$3::date,$4::date,$5::text,$6::text,$7::text,$8::text,$9::text,$10::text,$11::int4,$12::text,$13::int4,$14::int4,$15::int4)",
};

export async function playerStatFrame(context: Context): Promise<PsRow[]> {
  const c: Context = {
    ...context,
    starters: { offMin: null, offMax: null, defMin: null, defMax: null },
  };
  const euro = c.league !== "il";
  const comp = c.league === "eurocup" ? "U" : "E";
  const a = dbArgs(c);
  const raw = await withSnapshot(new Deadline(25000, 20000), (q) => {
    if (!needsFilteredPath(c))
      return euro
        ? q<Row>(SQL.elMv, [comp, c.season])
        : q<Row>(SQL.ilMv, [c.season]);
    return euro
      ? q<Row>(SQL.elGames, [
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
        ])
      : q<Row>(SQL.ilGames, [
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
          null,
          null,
          null,
          false,
          a.minGn,
          a.maxGn,
          a.lastN,
        ]);
  });
  return playerStatProjection(raw);
}
