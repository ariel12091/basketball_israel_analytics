import ui from "../data/shinyUi.json";
import type { League } from "../rules/gameLogs";
import { defaultSeason } from "./tabQuery";
import { comparePresetHref } from "./compareQuery";
export const homeTeams = (season: string) =>
  (ui.ilTeams as Record<string, { id: number; name: string }[]>)[season] ?? [];
export function homePath(season: string, team: string) {
  return `/api/v1/il/home?${new URLSearchParams({ season, team_id: team })}`;
}
export function homeTabHref(
  league: League,
  season: string,
  input: string,
  team: string,
) {
  const tab = input.replace(/^go_euro_/, "").replace(/^go_/, "");
  const path = (
    {
      onoff: "onoff",
      lineups: "lineups",
      team: "team-ratings",
      gamelogs: "game-logs",
      playerstats: "player-stats",
      compare: "compare",
    } as Record<string, string>
  )[tab];
  if (!path) throw Error("Unknown Home card");
  const q = new URLSearchParams({ season });
  // app.R go_*: team scoping only for On/Off, Lineups and Game Logs.
  if (team && ["onoff", "lineups", "game-logs"].includes(path))
    q.set(path === "game-logs" && league === "il" ? "team" : "teams", team);
  if (path === "lineups") q.set("size", "5");
  if (path === "compare") q.set("preset", "starters_bench");
  return `/${league}/${path}?${q}`;
}
export function homeStoryHref(season: string, preset: string, team: string) {
  return preset
    ? comparePresetHref(season, preset, team)
    : homeTabHref("il", season, "go_team", "");
}
export function homeLeagueHref(
  league: League,
  current: League,
  season: string,
) {
  return `/${league}/home?season=${league === current || (league !== "il" && current !== "il") ? season : defaultSeason(league)}`;
}
