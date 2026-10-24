import "server-only";
import { withSnapshot, Deadline, type Query } from "./db";
import type {
  GameLogParams,
  League,
  Row,
  SeasonInput,
} from "../rules/gameLogs";

// The existing Shiny readers, unchanged: gl_query_* in svc_game_logs.R,
// euro_fetch_game_log_rows in global_euro.R, fetch_scoreless_games in global.R.
export const QUERIES = {
  schedule:
    "SELECT * FROM basketball_test.final_schedule_mv WHERE game_year = $1",
  summary: `SELECT team_id, lineup_hash, type_lineup, g_date, game_id, game_year,
 total_poss, total_pts, fg2_made, fg2_att, fg3_made, fg3_att, minutes, num_starters
 FROM basketball_test.mv_lineup_totals_by_day WHERE game_year = $1`,
  ff: `SELECT lineup_hash, team_id, game_id, game_year, type_lineup,
 total_points, total_poss, ts_poss_count, oreb_count, oreb_opportunities, tov_count,
 total_ft_attempts, total_fga, total_fgm, total_fg3_made, minutes, num_starters
 FROM basketball_test.lineup_four_factors_by_game WHERE game_year = $1`,
  euro: `SELECT f.*, s.round_number, s.phase, s.game_date, s.opp_team_id,
 s.is_home, s.has_won, s.team_points, s.opp_points, s.team_name, s.opp_team_name,
 m.seconds / 60.0 AS minutes
 FROM euroleague.team_four_factors_by_game f
 JOIN euroleague.final_schedule_mv s ON s.game_id=f.game_id AND s.team_id=f.team_id
 LEFT JOIN (SELECT game_id, team_id, own_starters, opp_starters, SUM(seconds) AS seconds
 FROM euroleague.lineup_totals_by_game WHERE type_lineup='offense'
 AND competition=$1::text AND game_year=$2::int4
 GROUP BY game_id,team_id,own_starters,opp_starters) m
 ON m.game_id=f.game_id AND m.team_id=f.team_id
 AND m.own_starters=f.own_starters AND m.opp_starters=f.opp_starters
 WHERE s.competition=$1::text AND s.game_year=$2::int4`,
  // euro_fetch_seasons() in global_euro.R.
  euroSeasons: `SELECT DISTINCT game_year FROM euroleague.final_schedule_mv
 WHERE competition = $1::text ORDER BY game_year DESC`,
  scoreless: `SELECT game_id, team_id FROM basketball_test.df_pts_poss_lineups_longer_mv
 WHERE game_id = ANY($1::integer[])
 GROUP BY game_id, team_id HAVING COUNT(own_team_score)=0`,
  ilVersion: `SELECT value FROM basketball_test.app_meta WHERE key='etl_full_last_success'`,
  euroVersion: `SELECT COALESCE(MAX(completed_at),MAX(started_at))::text AS value
 FROM euroleague.load_runs WHERE status='completed'`,
};
export type Season = SeasonInput & { version: string; seasons?: string[] };
export async function readVersion(q: Query, league: League): Promise<string> {
  const result = await q<{ value: unknown }>(
    league === "il" ? QUERIES.ilVersion : QUERIES.euroVersion,
    [],
  );
  return result[0]?.value == null ? "unknown" : String(result[0].value);
}
export async function fetchSeason(q: Query, p: GameLogParams): Promise<Season> {
  const version = await readVersion(q, p.league);
  if (p.league !== "il") {
    const competition = p.league === "euroleague" ? "E" : "U";
    const rows = await q<Row>(QUERIES.euro, [competition, Number(p.season)]);
    const seasons = await q<{ game_year: unknown }>(QUERIES.euroSeasons, [
      competition,
    ]);
    return { rows, version, seasons: seasons.map((r) => String(r.game_year)) };
  }
  const schedule = await q<Row>(QUERIES.schedule, [Number(p.season)]);
  const rows = await q<Row>(
    p.view === "summary" ? QUERIES.summary : QUERIES.ff,
    [Number(p.season)],
  );
  // svc_game_logs.R gl_il_rows(): the scoring feed gates each Gameflow link.
  // Restrict the original scoreless reader to this season's games; the flag
  // still uses all feed rows for each game/team, independently of display filters.
  const gameIds = [...new Set(schedule.map((r) => Number(r.game_id)))].filter(
    (id) => Number.isInteger(id) && id > 0,
  );
  const scoreless = gameIds.length
    ? await q<Row>(QUERIES.scoreless, [gameIds])
    : [];
  return {
    rows,
    schedule,
    scoreless,
    ...(p.view === "four_factors" ? { ff: rows } : {}),
    version,
  };
}
const cache = new Map<string, { expires: number; promise: Promise<Season> }>();
/** A bounded warm-instance cache, plus the 60 s CDN cache. No publication claims. */
export function getSeason(p: GameLogParams): Promise<Season> {
  const key = `${p.league}/${p.season}/${p.league === "il" ? p.view : "both"}`,
    now = Date.now();
  const found = cache.get(key);
  if (found && found.expires > now) return found.promise;
  for (const [k, v] of cache) if (v.expires <= now) cache.delete(k);
  if (cache.size >= 4) cache.delete(cache.keys().next().value!);
  const promise = withSnapshot(new Deadline(), (q) => fetchSeason(q, p));
  cache.set(key, { expires: now + 60000, promise });
  void promise.catch(() => {
    if (cache.get(key)?.promise === promise) cache.delete(key);
  });
  return promise;
}
