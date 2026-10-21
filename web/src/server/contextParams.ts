// The shared game-context filters of the On/Off and Lineups tabs (inventory
// X-05), parsed fail-closed and mapped to the stored functions' arguments as
// game_context_db_args() / resolve_starters_bounds() in app/R/helpers.R do.
import type { League } from "../rules/gameLogs";
import { ParamError } from "./api";
import ui from "../data/shinyUi.json";

export type Context = {
  league: League;
  season: number;
  start: string;
  end: string;
  teams: number[];
  gameTypes: string[]; // Israeli game_type codes ("10,34" is one choice)
  phases: string[]; // EuroLeague phase text
  opponents: number[];
  homeAway: "" | "home" | "away";
  outcome: "" | "win" | "loss";
  rankSide: "" | "top" | "bottom";
  rankN: number | null;
  rankMetric: "" | "off" | "def" | "net";
  minGn: number | null;
  maxGn: number | null;
  lastN: number | null;
  starters: {
    offMin: number | null;
    offMax: number | null;
    defMin: number | null;
    defMax: number | null;
  };
};

const BOUNDS = ui.seasonDateBounds as Record<
  string,
  { start: string; end: string }
>;
/** season_date_bounds_for_year() / euro_season_date_bounds(). */
export function seasonBounds(league: League, season: number) {
  if (league === "il")
    return (
      BOUNDS[String(season)] ?? {
        start: `${season - 1}-09-01`,
        end: `${season}-07-01`,
      }
    );
  return { start: `${season}-09-01`, end: `${season + 1}-07-01` };
}

function int(
  v: string | null,
  name: string,
  min: number,
  max: number,
): number | null {
  if (v == null || v === "") return null;
  if (!/^\d+$/.test(v)) throw new ParamError(name, "Expected an integer");
  const n = Number(v);
  if (n < min || n > max) throw new ParamError(name, `Expected ${min}-${max}`);
  return n;
}
function ids(s: URLSearchParams, name: string): number[] {
  const v = (s.get(name) ?? "").split(",").filter(Boolean);
  if (v.length > 50) throw new ParamError(name, "At most 50 IDs");
  if (v.some((x) => !/^\d+$/.test(x))) throw new ParamError(name, "Invalid ID");
  return v.map(Number);
}
function date(v: string | null, name: string): string | null {
  if (!v) return null;
  if (
    !/^\d{4}-\d{2}-\d{2}$/.test(v) ||
    new Date(v + "T00:00:00Z").toISOString().slice(0, 10) !== v
  )
    throw new ParamError(name, "Invalid date");
  return v;
}
function oneOf<T extends string>(
  v: string | null,
  name: string,
  allowed: readonly T[],
): T {
  const x = (v ?? "") as T;
  if (!allowed.includes(x)) throw new ParamError(name, "Invalid value");
  return x;
}

export function parseContext(s: URLSearchParams, league: League): Context {
  const season =
    int(s.get("season"), "season", 2000, 2100) ??
    Number(league === "il" ? ui.defaultGameYear : ui.euroDefaultSeason);
  const b = seasonBounds(league, season);
  const start = date(s.get("start"), "start") ?? b.start;
  const end = date(s.get("end"), "end") ?? b.end;
  if (start > end) throw new ParamError("start", "Invalid date range");
  const starters = {
    offMin: null,
    offMax: null,
    defMin: null,
    defMax: null,
  } as Context["starters"];
  for (const [name, side] of [
    ["own_starters", "off"],
    ["opp_starters", "def"],
  ] as const) {
    const v = s.get(name);
    if (!v) continue;
    const m = /^(gte|lte):([0-5])$/.exec(v);
    if (!m) throw new ParamError(name, "Expected gte:N or lte:N, N=0-5");
    starters[`${side}${m[1] === "gte" ? "Min" : "Max"}`] = Number(m[2]);
  }
  const gameTypes = (s.get("game_type") ?? "").split(",").filter(Boolean);
  if (gameTypes.some((g) => !/^\d+$/.test(g)))
    throw new ParamError("game_type", "Invalid game type");
  const phases = (s.get("phase") ?? "").split(",").filter(Boolean);
  if (
    phases.length > 20 ||
    phases.some((p) => !/^[A-Za-z0-9 _-]{1,40}$/.test(p))
  )
    throw new ParamError("phase", "Invalid phase");
  const lastN = int(s.get("last_n"), "last_n", 1, 80);
  const minGn = int(s.get("min_gn"), "min_gn", 1, 80),
    maxGn = int(s.get("max_gn"), "max_gn", 1, 80);
  if (lastN && (minGn || maxGn))
    throw new ParamError(
      "last_n",
      "Last N and a game-number range are mutually exclusive",
    );
  return {
    league,
    season,
    start,
    end,
    teams: ids(s, "teams"),
    gameTypes: league === "il" ? gameTypes : [],
    phases: league === "il" ? [] : phases,
    opponents: ids(s, "opponents"),
    homeAway: oneOf(s.get("home_away"), "home_away", [
      "",
      "home",
      "away",
    ] as const),
    outcome: oneOf(s.get("outcome"), "outcome", ["", "win", "loss"] as const),
    rankSide: oneOf(s.get("opp_rank_side"), "opp_rank_side", [
      "",
      "top",
      "bottom",
    ] as const),
    rankN: int(s.get("opp_rank_n"), "opp_rank_n", 1, league === "il" ? 12 : 20),
    rankMetric: oneOf(s.get("opp_rank_metric"), "opp_rank_metric", [
      "",
      "off",
      "def",
      "net",
    ] as const),
    minGn,
    maxGn,
    lastN,
    starters,
  };
}

/** onoff_fallback_needed(): TRUE when the season view cannot answer. Team and
 * minimum-possession changes never leave the fast path. */
export function needsFilteredPath(c: Context): boolean {
  const b = seasonBounds(c.league, c.season);
  return (
    c.start !== b.start ||
    c.end !== b.end ||
    c.gameTypes.length > 0 ||
    c.phases.length > 0 ||
    c.opponents.length > 0 ||
    c.homeAway !== "" ||
    c.outcome !== "" ||
    c.rankSide !== "" ||
    Object.values(c.starters).some((v) => v != null) ||
    c.minGn != null ||
    c.maxGn != null ||
    c.lastN != null
  );
}

/** game_context_db_args(): blanks become NULL. */
export function dbArgs(c: Context) {
  const csv = (v: (string | number)[]) => (v.length ? v.join(",") : null);
  return {
    typeCsv: csv(c.league === "il" ? c.gameTypes : c.phases),
    oppCsv: csv(c.opponents),
    homeAway: c.homeAway || null,
    outcome: c.outcome || null,
    rankSide: c.rankSide || null,
    rankN: c.rankN,
    rankMetric: c.rankMetric || null,
    minGn: c.minGn,
    maxGn: c.maxGn,
    lastN: c.lastN,
    ...c.starters,
  };
}
