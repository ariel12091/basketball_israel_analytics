import "server-only";
import type { League, Row } from "../rules/gameLogs";
import { ribbonModel, type RawRibbon } from "../rules/gameflow";
import { gameflowSummary } from "../rules/gameflowSummary";
import sql from "../data/gameflowSql.json";
import { Deadline, withSnapshot } from "./db";
import { ParamError } from "./api";
import { readVersion } from "./gameLogsRepo";
import { VersionedCache } from "./versionedCache";
import { logEvent } from "../lib/logEvent";
import type { RawWinProb } from "../rules/gameflow";
import { ownerName, readPlayerNames } from "./playerNames";
import { readTeamIdentity } from "./teamIdentity";
import { applyTeamNamesHe, readTeamNamesHe, teamNameHe } from "./teamNames";
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
          `SELECT team_name,opp_team_name,opp_team_id,game_year,game_date::text AS game_date,${euro ? "team_points AS team_score,opp_points AS opp_score" : "team_score,opp_score"} FROM ${euro ? "euroleague" : "basketball_test"}.final_schedule_mv WHERE game_id=$1::int4 AND team_id=$2::int4${euro ? " AND competition=$3::text" : ""} LIMIT 1`,
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
    // Win probability after every possession (shared_definitions.md, Win
    // probability): the one owner, both leagues ('euro' covers EuroCup). A
    // failure here costs only the win chances, so it runs in a savepoint.
    await q("SAVEPOINT win_prob", []);
    try {
      raw.win_prob = await q<RawWinProb>(
        "SELECT possession_no, is_final, period, seconds_left, home_team_id, away_team_id, home_score, away_score, home_win_probability FROM basketball_test.game_win_probability($1::text, $2::int4)",
        [euro ? "euro" : "il", game],
      );
    } catch (e) {
      await q("ROLLBACK TO SAVEPOINT win_prob", []);
      logEvent("win_prob_unavailable", { league, code: (e as { code?: string }).code }, "warn");
    }
    // Names follow the display-name owner (D27); where it has none (a reused
    // provider id) the lane keeps this game's roster spelling. Twin:
    // fetch_stint_ribbon() in global.R.
    // Chat-only Hebrew names for the summary's player rows, by side:player_id.
    const hebrew = new Map<string, string>();
    if (!euro && raw.lanes?.length) {
      const names = await readPlayerNames(q, league, Number(meta.game_year));
      for (const l of raw.lanes) {
        const he = ownerName(names, l.team_id, l.player_id)?.he;
        if (he) hebrew.set(`${Number(l.team_id) === Number(team) ? "own" : "opp"}:${l.player_id}`, he);
      }
      raw.lanes = raw.lanes.map((l) => ({
        ...l,
        player_label:
          ownerName(names, l.team_id, l.player_id)?.en ?? l.player_label,
      }));
      // Each lineup's names in height order (basket.co.il).
      const seen = new Set<string>();
      raw.heights = raw.lanes.flatMap((l) => {
        const k = `${l.team_id}:${l.player_id}`,
          n = ownerName(names, l.team_id, l.player_id);
        if (seen.has(k) || n?.height == null) return [];
        seen.add(k);
        return [{ team_id: l.team_id, player_id: l.player_id, height_m: n.height, position_en: n.position ?? null }];
      });
    }
    // pg decodes the reader's jsonb arrays, unlike RPostgres's JSON strings.
    const model = ribbonModel(raw, team, league, game),
      ownTeam = String(meta.team_name ?? "Own"),
      oppTeam = String(meta.opp_team_name ?? "Opponent");
    // Runs and player ratings, once, for the page and the API/chat alike.
    const summary = gameflowSummary(model, { own: ownTeam, opp: oppTeam });
    // Each club's colours and logo (catalogue rows): Gameflow draws each side
    // in its own club's colours.
    const oppId = Number(meta.opp_team_id),
      identity = await readTeamIdentity(q, league, Number(meta.game_year), [team, oppId].filter(Number.isFinite));
    // Hebrew team names (server/teamNames.ts): each player row's team, and
    // both teams in meta for the runs. Empty for EuroLeague.
    const teamsHe = await readTeamNamesHe(q, league, Number(meta.game_year));
    const ownTeamHe = teamNameHe(teamsHe, ownTeam),
      oppTeamHe = teamNameHe(teamsHe, oppTeam);
    return {
      ...model,
      runs: summary.runs,
      players: applyTeamNamesHe(
        summary.players.map((p) => {
          const he = hebrew.get(`${p.side}:${p.player_key}`);
          return he ? { ...p, player_he: he } : p;
        }),
        teamsHe,
      ),
      meta: {
        version: await readVersion(q, league),
        league,
        game,
        team,
        season: String(meta.game_year),
        ownTeam,
        oppTeam,
        // The header's final score and date (the schedule's, as Game Logs).
        gameDate: meta.game_date == null ? null : String(meta.game_date),
        ownScore: meta.team_score == null ? null : Number(meta.team_score),
        oppScore: meta.opp_score == null ? null : Number(meta.opp_score),
        ...(identity.has(team) ? { ownIdentity: identity.get(team) } : {}),
        ...(identity.has(oppId) ? { oppIdentity: identity.get(oppId) } : {}),
        ...(ownTeamHe ? { ownTeamHe } : {}),
        ...(oppTeamHe ? { oppTeamHe } : {}),
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
