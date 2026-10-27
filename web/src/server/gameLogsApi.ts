import "server-only";
import { gameLogs, type League } from "../rules/gameLogs";
import { getSeason } from "./gameLogsRepo";
import { parseParams } from "./params";

/** The Game Logs answer for one request: the route's body, and the page's
 * first render (same parameters, same JSON). */
export async function gameLogsAnswer(league: League, search: URLSearchParams) {
  const p = parseParams(search, league);
  const season = await getSeason(p),
    result = gameLogs(season, season.scoreless ?? [], p);
  const schedule = season.schedule ?? season.rows;
  // fetch_teams_min() / euro_fetch_teams(): MIN(team_name) per team id,
  // ordered by name (the Israeli pickers use the catalogue roster first).
  const byId = new Map<number, string>();
  for (const row of schedule) {
    const id = Number(row.team_id),
      name = String(row.team_name);
    const seen = byId.get(id);
    if (seen === undefined || name < seen) byId.set(id, name);
  }
  const teams = [...byId]
    .map(([id, name]) => ({ id, name }))
    .sort((a, b) => (a.name < b.name ? -1 : a.name > b.name ? 1 : 0));
  return {
    ...result,
    meta: {
      ...result.meta,
      league,
      season: p.season,
      view: p.view,
      version: season.version,
      ...(season.seasons ? { seasons: season.seasons } : {}),
      teams,
      rounds: [
        ...new Set(
          schedule
            .map((row) => Number(row[league === "il" ? "gn" : "round_number"]))
            .filter(Number.isFinite),
        ),
      ].sort((a, b) => a - b),
      phases: [
        ...new Set(
          schedule.map((row) => String(row.phase ?? "")).filter(Boolean),
        ),
      ].sort(),
    },
  };
}
