import "server-only";
import { gameLogs, gameLogsOtherView, type League } from "../rules/gameLogs";
import { getSeason } from "./gameLogsRepo";
import { parseParams } from "./params";
import { applyTeamNamesHe, loadTeamNamesHe } from "./teamNames";
import { applyTeamLogos, loadTeamLogos } from "./teamIdentity";

/** The Game Logs answer for one request: the route's body, and the page's
 * first render (same parameters, same JSON). */
export async function gameLogsAnswer(league: League, search: URLSearchParams) {
  const p = parseParams(search, league);
  const season = await getSeason(p);
  // A filter on the other view's column reads that view's season too.
  const other = gameLogsOtherView(p);
  let otherRows;
  if (other) {
    const q = { ...p, view: other, stat_filters: [] };
    otherRows = gameLogs(await getSeason(q), [], q).rows;
  }
  const result = gameLogs(season, season.scoreless ?? [], p, otherRows);
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
  // Team and opponent logos (server/teamIdentity.ts) and Hebrew names
  // (server/teamNames.ts). The season pull is cached outside any snapshot, so
  // both come from their own reads, cached per data version.
  const withLogos = applyTeamLogos(result.rows, await loadTeamLogos(league, Number(p.season)));
  const rows =
    league === "il"
      ? applyTeamNamesHe(withLogos, await loadTeamNamesHe(league, Number(p.season)))
      : withLogos;
  return {
    ...result,
    rows,
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
      // The season's game types, for the chip that names what a default
      // page includes (Israeli league only).
      gameTypes:
        league === "il"
          ? [
              ...new Set(
                schedule
                  .map((row) => String(row.game_type ?? ""))
                  .filter(Boolean),
              ),
            ].sort()
          : [],
    },
  };
}
