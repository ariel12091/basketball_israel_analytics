import type {
  GameLogParams,
  League,
  StatFilter,
  Bounds,
} from "../rules/gameLogs";
import { ParamError } from "./api";
import ui from "../data/shinyUi.json";
export function leagueParam(value: string): League {
  if (!["il", "euroleague", "eurocup"].includes(value))
    throw new ParamError("league", "Unknown league");
  return value as League;
}
function integer(
  value: string | null,
  name: string,
  min: number,
  max: number,
): number | null {
  if (value === null || value === "") return null;
  if (!/^\d+$/.test(value)) throw new ParamError(name, "Expected an integer");
  const n = Number(value);
  if (n < min || n > max) throw new ParamError(name, `Expected ${min}–${max}`);
  return n;
}
function list(search: URLSearchParams, name: string): string[] {
  const values = (search.get(name) ?? "").split(",").filter(Boolean);
  if (values.length > 50) throw new ParamError(name, "At most 50 IDs");
  for (const id of values)
    if (!/^\d+$/.test(id)) throw new ParamError(name, "Invalid ID");
  return values;
}
export const FILTER_COLUMNS = [
  "gn",
  "minutes",
  "off_ppp",
  "def_ppp",
  "net_rtg",
  "Off Shot",
  "Def Shot",
  "off_fg2_pct",
  "off_fg3_pct",
  "off_fg2_freq",
  "off_fg3_freq",
  "def_fg2_pct",
  "def_fg3_pct",
  "def_fg2_freq",
  "def_fg3_freq",
  "off_poss",
  "def_poss",
  "off_efg_pct",
  "off_oreb_pct",
  "off_tov_pct",
  "off_ftr_pct",
  "def_efg_pct",
  "def_oreb_pct",
  "def_tov_pct",
  "def_ftr_pct",
];
export function parseParams(
  search: URLSearchParams,
  league: League,
): GameLogParams {
  const season =
    integer(search.get("season"), "season", 2000, 2100) ??
    // DEFAULT_GAME_YEAR / EURO_DEFAULT_SEASON, as Shiny opens on them.
    Number(league === "il" ? ui.defaultGameYear : ui.euroDefaultSeason);
  const view = search.get("view") ?? "summary";
  if (view !== "summary" && view !== "four_factors")
    throw new ParamError("view", "Invalid view");
  const dates = ["start", "end"].map((name) => {
    const value = search.get(name);
    if (!value) return null;
    if (
      !/^\d{4}-\d{2}-\d{2}$/.test(value) ||
      !Number.isFinite(Date.parse(value)) ||
      new Date(value).toISOString().slice(0, 10) !== value
    )
      throw new ParamError(name, "Invalid date");
    return value;
  });
  if (
    dates[0] &&
    dates[1] &&
    (dates[0] > dates[1] ||
      (Date.parse(dates[1]) - Date.parse(dates[0])) / 86400000 > 430)
  )
    throw new ParamError("dates", "Invalid date range");
  const starters: Bounds = {};
  for (const [name, side] of [
    ["own_starters", "off"],
    ["opp_starters", "def"],
  ] as const) {
    const text = search.get(name);
    if (text) {
      const match = /^(gte|lte):([0-5])$/.exec(text);
      if (!match) throw new ParamError(name, "Expected gte:N or lte:N, N=0–5");
      starters[`${side}_${match[1] === "gte" ? "min" : "max"}`] = Number(
        match[2],
      );
    }
  }
  const gn = {
    min_gn: integer(search.get("min_gn"), "min_gn", 1, 80),
    max_gn: integer(search.get("max_gn"), "max_gn", 1, 80),
    last_n: integer(search.get("last_n"), "last_n", 1, 80),
  };
  if (gn.last_n && (gn.min_gn || gn.max_gn))
    throw new ParamError(
      "last_n",
      "Last N and round range are mutually exclusive",
    );
  if (gn.min_gn && gn.max_gn && gn.min_gn > gn.max_gn)
    throw new ParamError("min_gn", "Range is reversed");
  const home_away = search.get("home_away") ?? "",
    outcome = search.get("outcome") ?? "";
  if (!["", "home", "away"].includes(home_away))
    throw new ParamError("home_away", "Invalid home/away");
  if (!["", "win", "loss"].includes(outcome))
    throw new ParamError("outcome", "Invalid outcome");
  let filters: StatFilter[] = [];
  if (search.get("stats")) {
    try {
      filters = JSON.parse(search.get("stats")!);
    } catch {
      throw new ParamError("stats", "Invalid stat filters");
    }
    if (
      !Array.isArray(filters) ||
      filters.length > 2 * FILTER_COLUMNS.length ||
      filters.some(
        (f) =>
          !f ||
          !FILTER_COLUMNS.includes(f.col) ||
          !["ge", "le"].includes(f.op) ||
          typeof f.value !== "number" ||
          !Number.isFinite(f.value),
      )
    )
      throw new ParamError("stats", "Invalid stat filters");
  }
  const teams = list(search, "teams"),
    team = search.get("team") ?? "";
  if (team && !/^\d+$/.test(team))
    throw new ParamError("team", "Invalid team ID");
  return {
    league,
    season,
    view,
    team,
    teams: teams.map(Number),
    dates: search.has("start") || search.has("end") ? dates : null,
    game_type: list(search, "game_type"),
    phase: (search.get("phase") ?? "").split(",").filter(Boolean),
    opponents: list(search, "opponents"),
    home_away,
    outcome,
    starters,
    gn,
    stat_filters: league === "il" ? filters : [],
  };
}
