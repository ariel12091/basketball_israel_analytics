// The columns the ask box may quote, each with the formatter its page uses.
// A copy of the pages' column choices, pinned by display.test.ts: every key's
// text equals the page's own rendered cell for the same row.
import homeUi from "@/data/homeUi.json";
import ui from "@/data/shinyUi.json";
import type { League } from "@/rules/gameLogs";
import { formatNumber } from "@/rules/format";
import { formatCompare, gridGapText } from "@/rules/compare";
import { homeFixed, homeOrdinal } from "@/rules/home";
import { SP_LABELS, SP_SUFFIX } from "@/rules/onoff";
import { PLAYER_LABELS } from "@/rules/playerStatsTable";
import { rRound } from "@/rules/round";
import type { ToolName } from "./registry";
import type { AskRow } from "./run";

type Fmt = (v: unknown, cells: Record<string, unknown>) => string;
export type ColumnSpec = { label: string; format: Fmt };

const num = (v: unknown) => (v == null || v === "" ? null : Number(v));
const fixed =
  (d: number): Fmt =>
  (v) =>
    formatNumber(v, d);
const text: Fmt = (v) => (v == null ? "" : String(v));
/** TeamRatingsTable metric(): one decimal, "NA" (or "--" and "%" in Shot Profile). */
const rated =
  (shot: boolean): Fmt =>
  (v) =>
    v == null ? (shot ? "--" : "NA") : rRound(Number(v), 1).toFixed(1) + (shot ? "%" : "");
/** The page's "#3" (or "#NA") line under a Team Ratings value. */
const rank: Fmt = (v) => `#${v ?? "NA"}`;
/** OnOffTables signed(v, 2). */
const signed2: Fmt = (v) => {
  const n = num(v);
  if (n == null || Number.isNaN(n)) return v == null ? "" : String(v);
  return (n > 0 ? "+" : "") + n.toFixed(2);
};
/** GameLogsTable value(): one decimal for minutes and ui.round1, whole possessions. */
const gameLog =
  (view: string, id: string): Fmt =>
  (v) =>
    typeof v !== "number"
      ? v == null
        ? ""
        : String(v)
      : id === "minutes" || (ui.round1 as Record<string, string[]>)[view]?.includes(id)
        ? formatNumber(v, 1)
        : id === "off_poss" || id === "def_poss"
          ? formatNumber(v, 0)
          : String(v);

const spec = (entries: [string, string, Fmt][]) =>
  Object.fromEntries(entries.map(([k, label, format]) => [k, { label, format }]));
const FF = [
  ["ppp", "PPP"],
  ["efg", "eFG%"],
  ["oreb", "OREB%"],
  ["tov", "TOV%"],
  ["ftr", "FTR"],
] as const;
const sides = ["off", "def"] as const;
const SIDE = { off: "Off", def: "Def" };
/** A Team Ratings metric with its rank (the page shows both in one cell). */
const metric = (id: string, label: string, shot = false): [string, string, Fmt][] => [
  [id, label, rated(shot)],
  // Shot Profile shows a missing value as "--" with no rank beneath it.
  [`rank_${id}`, `${label} rank`, shot ? (v, c) => (c[id] == null ? "--" : rank(v, c)) : rank],
];
const TRAD = [
  "pts", "reb", "oreb", "dreb", "ast", "stl", "blk", "dfl", "tov", "fgm", "fga",
  "fg_pct", "2pm", "2pa", "two_pct", "3pm", "3pa", "tp_pct", "ftm", "fta",
  "ft_pct", "efg", "ts",
];

function build(tool: ToolName, league: League, view: string | null): Record<string, ColumnSpec> {
  switch (tool) {
    case "lineups":
      return view === "four_factors"
        ? spec([
            ["Team", "Team", text],
            ["Players", "Players", text],
            ...sides.flatMap((s) => [
              ...FF.map(([m, l]) => [`${s}_${m}`, `${SIDE[s]} ${l}`, fixed(1)] as [string, string, Fmt]),
              [`${s}_poss`, `${SIDE[s]} Poss`, fixed(0)] as [string, string, Fmt],
            ]),
            ["minutes", "Min", fixed(1)],
            ["total_poss", "Poss", fixed(0)],
            ["net_rtg", "Net", fixed(1)],
          ])
        : spec([
            ["Team", "Team", text],
            ["Players", "Players", text],
            ["minutes", "Min", fixed(1)],
            ["total_poss", "Total Poss", fixed(0)],
            ["off_ppp", "Off PPP", fixed(1)],
            ["def_ppp", "Def PPP", fixed(1)],
            ["net_rtg", "Net RTG", fixed(1)],
            ["plus_minus", "+/-", fixed(0)],
            ["off_poss", "Off Poss", fixed(0)],
            ["off_pts", "Off Pts", fixed(0)],
            ["def_poss", "Def Poss", fixed(0)],
            ["def_pts", "Def Pts", fixed(0)],
          ]);
    case "team_ratings":
      if (view === "traditional")
        return spec([
          ["team_name", "Team", text],
          ["gp", "GP", fixed(1)],
          ["poss_on_floor", "Poss On Floor", fixed(1)],
          ["minutes", "Min", fixed(1)],
          ...TRAD.filter((m) => league === "il" || m !== "dfl").flatMap((m) =>
            metric(m, PLAYER_LABELS[m] ?? m),
          ),
        ]);
      if (view === "shot_profile")
        return spec([
          ["team_name", "Team", text],
          ["minutes", "Min", fixed(1)],
          ...sides.flatMap((s) => [
            ...SP_SUFFIX.flatMap((m, i) =>
              metric(`${s}_${m}`, `${SIDE[s]} ${i === 5 ? "Corner 3 Share" : SP_LABELS[i]}`, true),
            ),
            [`${s}_poss`, `${SIDE[s]} Poss`, fixed(0)] as [string, string, Fmt],
          ]),
        ]);
      if (view === "four_factors")
        return spec([
          ["team_name", "Team", text],
          ["minutes", "Min", fixed(1)],
          ...sides.flatMap((s) => [
            ...FF.flatMap(([m, l]) => metric(`${s}_${m}`, `${SIDE[s]} ${l}`)),
            [`${s}_poss`, `${SIDE[s]} Poss`, fixed(0)] as [string, string, Fmt],
          ]),
          ...metric("net_rtg", "Net"),
        ]);
      return spec([
        ["team_name", "Team", text],
        ["games_played", "GP", fixed(0)],
        ["minutes", "Min", fixed(1)],
        ["wins", "W", fixed(0)],
        ["losses", "L", fixed(0)],
        ...metric("off_ppp", "Off PPP"),
        ...metric("def_ppp", "Def PPP"),
        ...metric("net_rtg", "Net Rtg"),
        ["off_pace", "Off Pace", fixed(1)],
        ["def_pace", "Def Pace", fixed(1)],
        ["off_poss", "Off Poss", fixed(0)],
        ["def_poss", "Def Poss", fixed(0)],
      ]);
    case "player_stats":
      return spec([
        ["team_name", "Team", text],
        ["Player", "Player", text],
        ...Object.entries(PLAYER_LABELS)
          .filter(([id]) => league === "il" || id !== "dfl")
          .map(([id, l]) => [id, l, fixed(id === "gp" ? 0 : 1)] as [string, string, Fmt]),
      ]);
    case "game_logs": {
      const v = view === "four_factors" ? "four_factors" : "summary";
      const ids =
        v === "four_factors"
          ? [
              "minutes",
              "net_rtg",
              ...sides.flatMap((s) =>
                ["ppp", "efg_pct", "oreb_pct", "tov_pct", "ftr_pct"].map((m) => `${s}_${m}`),
              ),
            ]
          : ["minutes", "off_poss", "def_poss", "off_ppp", "def_ppp", "net_rtg"];
      return spec([
        [league === "il" ? "game_type_label" : "phase_label", "Game Type", text],
        ["game_date", "Date", text],
        ["opp_team_name", "Opponent", text],
        [league === "il" ? "score_display" : "score", "Score", text],
        ...ids.map((id) => [id, id, gameLog(v, id)] as [string, string, Fmt]),
      ]);
    }
    case "onoff":
      if (view !== "summary" && view !== null) return {};
      return spec([
        ["Team", "Team", text],
        ["Player", "Player", text],
        ["Net RTG Diff", "Net impact (on-off)", signed2],
        ["Off ON Diff", "Off impact", signed2],
        ["Def ON Diff", "Def impact", signed2],
        ["Off ON PPP", "Off PPP on court", fixed(1)],
        ["Def ON PPP", "Def PPP on court", fixed(1)],
        ["On Net RTG", "Net on court", signed2],
        ["Off OFF PPP", "Off PPP off court", fixed(1)],
        ["Def OFF PPP", "Def PPP off court", fixed(1)],
        ["Off Net RTG", "Net off court", signed2],
        ["minutes", "Min", fixed(1)],
        ["ON Poss", "On Poss", fixed(0)],
        ["OFF Poss", "Off Poss", fixed(0)],
      ]);
    case "compare":
      // CompareGrid's cells (EntityDetails), per row's own format.
      if (view === "detail")
        return spec([
          ["section", "Section", text],
          ["label", "Metric", text],
          ["a", "Side A", (v, r) => formatCompare(v, String(r.fmt), r.fmt === "net")],
          ["b", "Side B", (v, r) => formatCompare(v, String(r.fmt), r.fmt === "net")],
          ["gap", "Gap", (v, r) => gridGapText(v == null ? null : Number(v), String(r.fmt))],
          ["better", "Ahead", text],
        ]);
      // CompareClient's team players table.
      if (view === "players") {
        const fmt = (r: Record<string, unknown>) => (r.pct ? "pct" : "num");
        const played = (side: "a" | "b"): Fmt => (v, r) =>
          `${formatCompare(v, fmt(r))}${r[`played_${side}`] ? "" : " (didn't play)"}`;
        return spec([
          ["rank", "#", text],
          ["player_name", "Player", text],
          ["gp_a", "GP A", (v, r) => String(r.played_a ? v : 0)],
          ["gp_b", "GP B", (v, r) => String(r.played_b ? v : 0)],
          ["metric_a", "Side A", played("a")],
          ["metric_b", "Side B", played("b")],
          ["poss_a", "Total Poss A", text],
          ["poss_b", "Total Poss B", text],
          ["gap", "Gap", (v, r) => formatCompare(v, fmt(r), true)],
        ]);
      }
      return spec([
        ["rank", "#", text],
        ["entity_name", "Team or lineup", text],
        ["team_name", "Team", text],
        ["metric_a", "Side A", (v) => formatCompare(v, "num", false)],
        ["metric_b", "Side B", (v) => formatCompare(v, "num", false)],
        ["gap", "Gap", (v) => formatCompare(v, "num", true)],
        ["poss_a", "Total Poss A", (v) => String(Math.trunc(Number(num(v))))],
        ["poss_b", "Total Poss B", (v) => String(Math.trunc(Number(num(v))))],
      ]);
    case "list_teams":
      return spec([["name", "Team", text]]);
    // The lineup filter's player picker shows the name only.
    case "lineup_roster":
      return spec([["name", "Player", text]]);
    case "team_overview":
      return spec([
        ["section", "Section", text],
        ["team_name", "Team", text],
        ["wins", "W", text],
        ["losses", "L", text],
        ["off_ppp", "Off PPP", (v) => homeFixed(v)],
        ["def_ppp", "Def PPP", (v) => homeFixed(v)],
        ["net_rtg", "Net", (v) => homeFixed(v, true)],
        ["rank_off_ppp", "Off rank", (v) => homeOrdinal(v)],
        ["rank_def_ppp", "Def rank", (v) => homeOrdinal(v)],
        ["rank_net_rtg", "Net rank", (v) => homeOrdinal(v)],
        ["n_teams", "Teams", text],
        ["label", "Factor", text],
        ["value", "Value", (v) => homeFixed(v)],
        ["rank", "Rank", (v) => homeOrdinal(v)],
        ["Player", "Player", text],
        ["player_name", "Player", text],
        ["Net RTG Diff", "Net impact", (v) => homeFixed(v, true)],
        ["ppg", "PPG", (v) => homeFixed(v)],
        ["player_names_str", "Lineup", text],
        ["text", "Storyline", text],
        ["result", "Result", text],
        ["context", "Game", text],
      ]);
    default:
      return {};
  }
}

const TOOLTIPS = ui.columnTooltips as Record<string, string>;
/** A quotable column as the model sees it: its label and the app's own
 * definition (COLUMN_TOOLTIPS), e.g. "Off PPP: Offensive points per 100 possessions". */
export function columnDefinition(label: string): string {
  const def = TOOLTIPS[label] ?? TOOLTIPS[label.replace(/^(Off|Def) /, "")];
  return def ? `${label}: ${def}` : label;
}

const cache = new Map<string, Record<string, ColumnSpec>>();
export function displaySpec(
  tool: ToolName,
  league: League,
  view: string | null,
): Record<string, ColumnSpec> {
  const key = `${tool}|${league === "il" ? "il" : "euro"}|${view}`;
  let s = cache.get(key);
  if (!s) cache.set(key, (s = build(tool, league, view)));
  return s;
}

/** The page's text for one quotable cell; null when the column is not quotable. */
export function cellText(
  tool: ToolName,
  league: League,
  view: string | null,
  key: string,
  row: AskRow,
  lang: "he" | "en" = "en",
): string | null {
  const c = displaySpec(tool, league, view)[key];
  if (!c) return null;
  const text = c.format(row.cells[key], row.cells);
  return lang === "he" ? inHebrew(tool, key, text) : text;
}

/** The Home hub's English-only pieces in a Hebrew answer: the same rank as
 * "#1" instead of "1st", and the last game's "W"/"L" as ניצחון/הפסד. */
function inHebrew(tool: ToolName, key: string, text: string): string {
  if (tool !== "team_overview") return text;
  if (/^rank/.test(key)) return text.replace(/^(\d+)(?:st|nd|rd|th)$/, "#$1");
  if (key === "result") return text.replace(/^W /, "ניצחון ").replace(/^L /, "הפסד ");
  // homeLastGame(): "vs|at <team> · <Mon> <day>" -> "בבית|בחוץ מול <team> · <day>/<month>".
  if (key === "context") {
    const m = text.match(/^(vs|at) (.+) · (\w{3}) (\d{1,2})$/);
    const month = m ? (homeUi.months as string[]).indexOf(m[3]) + 1 : 0;
    if (m && month > 0) return `${m[1] === "vs" ? "בבית מול" : "בחוץ מול"} ${m[2]} · ${m[4]}/${month}`;
  }
  return text;
}
