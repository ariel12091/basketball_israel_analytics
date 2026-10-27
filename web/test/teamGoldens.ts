import { readdirSync, readFileSync, existsSync } from "node:fs";
import type { Row, League, StatFilter } from "../src/rules/gameLogs";
import { teamTableRows, type TeamView } from "../src/rules/teamRatings";
import { rRound } from "../src/rules/round";
export const DIR = "test/goldens/team_ratings";
export const teamPresets = existsSync(DIR)
  ? readdirSync(DIR).filter(
      (f) => f.endsWith(".json") && !f.endsWith(".answer.json"),
    )
  : [];
export function loadTeam(file: string) {
  const g = JSON.parse(readFileSync(`${DIR}/${file}`, "utf8")) as {
    id: string;
    league: League;
    view: TeamView;
    query: Record<string, string>;
    filters: StatFilter[];
    expected: { data: Row[]; hidden: string[] };
  };
  const answer = JSON.parse(
    readFileSync(`${DIR}/${g.id}.answer.json`, "utf8"),
  ) as {
    rows: Row[];
    previous: Row[];
    meta: { version: string; showDelta: boolean };
  };
  return {
    g,
    answer,
    rows: teamTableRows(
      answer.rows,
      answer.previous,
      g.view,
      g.filters,
      answer.meta.showDelta,
      g.query.side === "defense",
    ),
  };
}
const traditionalLabels: Record<string, string> = {
  Team: "team_name",
  GP: "gp",
  "Poss On Floor": "poss_on_floor",
  Min: "minutes",
  PTS: "pts",
  REB: "reb",
  OREB: "oreb",
  DREB: "dreb",
  AST: "ast",
  STL: "stl",
  BLK: "blk",
  DFL: "dfl",
  TOV: "tov",
  FGM: "fgm",
  FGA: "fga",
  "FG%": "fg_pct",
  "2PM": "2pm",
  "2PA": "2pa",
  "2P%": "two_pct",
  "3PM": "3pm",
  "3PA": "3pa",
  "3P%": "tp_pct",
  FTM: "ftm",
  FTA: "fta",
  "FT%": "ft_pct",
  "eFG%": "efg",
  "TS%": "ts",
};
/** Decode the oracle's value/#rank/arrow markup into semantic fields. */
export function teamOracleChecks(
  expected: Row,
  actual: Row,
  view: TeamView,
): [string, unknown, unknown][] {
  const checks: [string, unknown, unknown][] = [];
  for (const [key, value] of Object.entries(expected)) {
    if (key.startsWith("sort") || key.startsWith("rank_")) continue;
    let metric = view === "traditional" ? (traditionalLabels[key] ?? key) : key;
    if (
      view === "shot_profile" &&
      !key.startsWith("pr_") &&
      /^(off|def)_/.test(key) &&
      !key.endsWith("_poss") &&
      !key.endsWith("_efg")
    )
      metric = key.endsWith("_c3") ? key + "_pct3" : key + "_share";
    if (
      view === "shot_profile" &&
      key.startsWith("pr_") &&
      !key.endsWith("_efg")
    )
      metric = key.endsWith("_c3") ? key + "_pct3" : key + "_share";
    if (typeof value === "string" && value.includes("<br>")) {
      const [v, rank, delta] = value.split("<br>");
      checks.push([
        key,
        actual[metric] == null
          ? "NA"
          : rRound(Number(actual[metric]), 1).toFixed(1),
        v.replace("%", "").trim(),
      ]);
      checks.push([
        key + " rank",
        actual[`rank_${metric}`] ?? null,
        rank === "#NA" ? null : Number(rank.slice(1)),
      ]);
      if (view !== "shot_profile")
        checks.push([
          key + " delta",
          actual[`delta_${metric}`] ?? null,
          delta === "&#8212;"
            ? null
            : delta === "&#8596;"
              ? 0
              : (delta?.startsWith("&#9650;") ? 1 : -1) *
                Number(delta?.replace(/&#\d+;/g, "")),
        ]);
    } else if (value === "--") checks.push([key, actual[metric] ?? null, null]);
    else checks.push([key, actual[metric] ?? null, value]);
  }
  return checks;
}
