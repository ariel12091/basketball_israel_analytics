import "server-only";
import type { League, Row } from "../rules/gameLogs";
import { ribbonModel, type RawRibbon } from "../rules/gameflow";
import sql from "../data/gameflowSql.json";
import { Deadline, withSnapshot } from "./db";
import { ParamError } from "./api";
import { readVersion } from "./gameLogsRepo";
import { VersionedCache } from "./versionedCache";
import { ownerName, readPlayerNames } from "./playerNames";
export function parseGameflow(search: URLSearchParams) {
  const id = (key: string) => {
    const value = search.get(key) ?? "";
    if (!/^[1-9]\d*$/.test(value) || Number(value) > 2147483647)
      throw new ParamError(key, "Expected a positive game/team ID");
    return Number(value);
  };
  return { game: id("game_id"), team: id("team_id") };
}
async function read(league: League, game: number, team: number) {
  return withSnapshot(new Deadline(25000, 20000), async (q) => {
    const euro = league !== "il",
      meta = (
        await q<Row>(
          `SELECT team_name,opp_team_name,game_year FROM ${euro ? "euroleague" : "basketball_test"}.final_schedule_mv WHERE game_id=$1::int4 AND team_id=$2::int4${euro ? " AND competition=$3::text" : ""} LIMIT 1`,
          euro ? [game, team, league === "eurocup" ? "U" : "E"] : [game, team],
        )
      )[0];
    if (!meta)
      throw new ParamError(
        "game_id",
        "Game/team not found in this competition",
      );
    const raw = (
      await q<RawRibbon>(euro ? sql.euro : sql.il, [game, team])
    )[0] ?? { lanes: [], margin: [], n_periods: 4 };
    // Names follow the display-name owner (D27); where it has none (a reused
    // provider id) the lane keeps this game's roster spelling. Twin:
    // fetch_stint_ribbon() in global.R.
    if (!euro && raw.lanes?.length) {
      const names = await readPlayerNames(q, league, Number(meta.game_year));
      raw.lanes = raw.lanes.map((l) => ({
        ...l,
        player_label:
          ownerName(names, l.team_id, l.player_id)?.en ?? l.player_label,
      }));
    }
    // pg decodes the reader's jsonb arrays, unlike RPostgres's JSON strings.
    return {
      ...ribbonModel(raw, team, league, game),
      meta: {
        version: await readVersion(q, league),
        league,
        game,
        team,
        season: String(meta.game_year),
        ownTeam: String(meta.team_name ?? "Own"),
        oppTeam: String(meta.opp_team_name ?? "Opponent"),
      },
    };
  });
}
export type GameflowPayload = Awaited<ReturnType<typeof read>>;
const cache = new VersionedCache<GameflowPayload>(
  32,
  (value) => value.meta.version,
);
export function gameflowAnswer(league: League, s: URLSearchParams) {
  const { game, team } = parseGameflow(s),
    key = `${league}:${game}:${team}`;
  return cache.get(league, key, () => read(league, game, team));
}
