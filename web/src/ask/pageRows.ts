// The table rows a freshly opened link shows. Each answer function returns the
// page's population; the page narrows and derives its table in the browser with
// the rules below, in this order and with these defaults (LineupsClient,
// PlayerStatsClient, TeamRatingsClient, OnOffClient). Keep the two in step.
import ui from "@/data/shinyUi.json";
import type { League, Row } from "@/rules/gameLogs";
import { lineupPage, playerIds } from "@/rules/lineups";
import {
  autoBars,
  autoShare,
  onoffTableRows,
  type View,
} from "@/rules/onoff";
import {
  PLAYER_MODES,
  playerDisplay,
  type PlayerMode,
} from "@/rules/playerStatsTable";
import { teamTableRows, type TeamView } from "@/rules/teamRatings";
import { parseStats } from "@/lib/tabQuery";
import type { ToolName } from "./registry";
import { applyPlayerStatFilters, type PsRow } from "@/rules/playerStats";

const list = (v: string | null) => (v ?? "").split(",").filter(Boolean);

/** The page's auto min-poss (D34): a bar the request names is hand-set and
 * stays; otherwise auto's value applies, up or down, falling back to the
 * default when auto has nothing to measure. */
export function autoBar(
  asked: string | null,
  fallback: number,
  needed: number | null,
): number {
  if (asked != null && asked !== "") return Number(asked) || 0;
  return needed ?? fallback;
}

type Payload = {
  rows?: Row[];
  previous?: Row[];
  identities?: unknown[];
  league3p?: number | null;
  /** On/Off Player Stats chips: the player-stat-filters frame (run.ts). */
  psRows?: unknown[];
  meta?: { view?: string; filtered?: boolean; showDelta?: boolean };
};

export function pageRows(
  tool: ToolName,
  league: League,
  payload: unknown,
  q: URLSearchParams,
): Row[] {
  const p = (payload ?? {}) as Payload;
  const rows = p.rows ?? [];
  const stats = parseStats(q.get("stats") ?? "");
  if (tool === "lineups") {
    // lineupsPageAnswer's composition; a link opens with auto on, which
    // replaces the link's min_poss.
    return lineupPage(rows, {
      team: q.get("team") ?? "",
      selection: {
        on: playerIds(q.get("players_on")),
        any: playerIds(q.get("players_any")),
        off: playerIds(q.get("players_off")),
        count: Number(q.get("any_count")) || 1,
        exact: q.get("any_exact") === "true",
      },
      view: q.get("view") === "four_factors" ? "four_factors" : "summary",
      min: null,
      stats,
    }).rows;
  }
  if (tool === "player_stats") {
    const mode = (
      PLAYER_MODES.includes(q.get("view") as PlayerMode)
        ? q.get("view")
        : "Per Game"
    ) as PlayerMode;
    const maxGp = Math.max(1, ...rows.map((r) => Number(r.gp) || 0));
    const minGp = Math.min(
      maxGp,
      Math.max(1, Math.trunc(Number(q.get("min_gp")) || 1)),
    );
    return playerDisplay(rows, (p.identities ?? []) as never, {
      mode,
      players: list(q.get("players")),
      minGp,
      showIneligible: q.get("show_ineligible") === "true",
      filters: stats,
    }).rows;
  }
  if (tool === "team_ratings")
    return teamTableRows(
      rows,
      p.previous ?? [],
      (p.meta?.view ?? q.get("view") ?? "summary") as TeamView,
      stats,
      p.meta?.showDelta ?? false,
      q.get("side") === "defense",
    );
  if (tool === "onoff") {
    const psFilters = stats.filter((f) => f.col.startsWith("ps_"));
    const view = (p.meta?.view ?? "summary") as View;
    const defaults =
      league === "il" ? ui.onoffDefaults.israel : ui.onoffDefaults.euroleague;
    const teams = list(q.get("teams")).map(Number);
    const base =
      p.meta?.filtered && view === "summary"
        ? rows
        : teams.length
          ? rows.filter((r) => teams.includes(Number(r.team_id)))
          : rows;
    const bars = autoBars(base, view, autoShare(teams));
    const minOn = autoBar(q.get("min_on"), defaults.minOn, bars?.minOn ?? null);
    const minAll = autoBar(
      q.get("min_all"),
      defaults.minAll,
      bars?.minAll ?? null,
    );
    const shown = onoffTableRows(
      { rows, league3p: p.league3p ?? null, view },
      { teams, minAll, minOn, filters: stats },
      psFilters.length
        ? (r) =>
            applyPlayerStatFilters(r, (p.psRows ?? []) as PsRow[], psFilters)
        : undefined,
    );
    return withTeamNetRank(shown, view === "four_factors" ? "Net Diff" : "Net RTG Diff");
  }
  return rows;
}

/** For the chat only ("his net impact is #5 on the team"): each row's net
 * impact rank among its team's rows in the table the page shows, ties
 * sharing the better rank, and how many of the team's rows are ranked. The
 * model may not count rows itself, so the app supplies the rank. */
export function withTeamNetRank(rows: Row[], col: string): Row[] {
  const value = (r: Row) => (r[col] == null || r[col] === "" ? NaN : Number(r[col]));
  const byTeam = new Map<string, number[]>();
  for (const r of rows) {
    const v = value(r);
    if (!Number.isFinite(v)) continue;
    const k = String(r.team_id);
    byTeam.set(k, [...(byTeam.get(k) ?? []), v]);
  }
  return rows.map((r) => {
    const v = value(r),
      team = byTeam.get(String(r.team_id)) ?? [];
    return Number.isFinite(v)
      ? { ...r, team_rank_net: 1 + team.filter((x) => x > v).length, team_ranked: team.length }
      : r;
  });
}
