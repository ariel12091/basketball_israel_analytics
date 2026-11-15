// Each answer function's payload as plain rows for the ask tools. No values
// are derived here except through the pages' own rules (rules/home.ts).
import { homeDashboard, type HomeFrames } from "@/rules/home";
import type { League } from "@/rules/gameLogs";
import type { ExplainedRun, PlayerRating } from "@/rules/gameflowSummary";
import { periodClock } from "@/rules/gameflowRuns";
import type { ToolName } from "./registry";

type Cells = Record<string, unknown>;
export type Flat = { rows: Cells[]; total: Cells | null };

export function flatten(
  tool: ToolName,
  league: League,
  payload: unknown,
  query: URLSearchParams,
): Flat {
  const p = (payload ?? {}) as { rows?: Cells[]; frames?: HomeFrames };
  if (tool === "gameflow") return { rows: gameflowRows(payload), total: null };
  if (tool === "team_overview")
    return { rows: overview(p.frames, query.get("team_id") ?? ""), total: null };
  // Table tools read the page's finished table (shape=table); On/Off adds the
  // chat's on-team net impact rank.
  const view = (payload as { meta?: { view?: string } } | null)?.meta?.view;
  const rows: Cells[] =
    tool === "onoff"
      ? withTeamNetRank(p.rows ?? [], view === "four_factors" ? "Net Diff" : "Net RTG Diff")
      : (p.rows ?? []);
  if (tool === "lineups") {
    // LineupTable: the is_total === 0 row is the pinned "- All Lineups -" TOTAL.
    const total = rows.find((r) => r.is_total === 0) ?? null;
    return { rows: rows.filter((r) => r !== total), total };
  }
  if (tool === "lineup_roster" && query.get("team")) {
    // The route lists every team (the lineup filter's pool); the picker on
    // the page shows one team's players.
    const team = query.get("team");
    return { rows: rows.filter((r) => String(r.team_id) === team), total: null };
  }
  return { rows, total: null };
}

/** Gameflow: one row per run (the page's chip and tooltip) and one per
 * player (the card's whole-game ratings), from the answer's own summary
 * (rules/gameflowSummary.ts). */
function gameflowRows(payload: unknown): Cells[] {
  const p = (payload ?? {}) as {
    runs?: ExplainedRun[];
    players?: (PlayerRating & { player_he?: string })[];
    bounds?: number[];
    meta?: { ownTeam?: string; oppTeam?: string };
  };
  const bounds = p.bounds ?? [];
  const runs = (p.runs ?? []).map((r) => ({
    section: "run",
    team: r.side === "own" ? p.meta?.ownTeam : p.meta?.oppTeam,
    run_score: `${r.pts}-${r.oppPts}`,
    window: `${periodClock(r.start, bounds)} – ${periodClock(r.end, bounds)}`,
    defining: r.defining ? "yes" : "no",
    explanation: r.lines.join(" | "),
  }));
  const players = [...(p.players ?? [])]
    .sort((a, b) => b.secs - a.secs)
    .map((x) => ({
      section: "player",
      team: x.team,
      player: x.player,
      ...(x.player_he ? { player_he: x.player_he } : {}),
      secs: x.secs,
      pm: x.pm,
      net: x.net,
      ortg: x.ortg,
      drtg: x.drtg,
      off_poss: x.off_poss,
      def_poss: x.def_poss,
    }));
  return [...runs, ...players];
}

/** The Home hub's sections (homeDashboard) as one row each. */
function overview(frames: HomeFrames | undefined, team: string): Cells[] {
  if (!frames || !team) return [];
  const d = homeDashboard(frames, team);
  const out: Cells[] = [];
  // HomeHub shows the frame's own rank_off_ppp / rank_def_ppp / rank_net_rtg.
  if (d.identity)
    out.push({
      section: "identity",
      ...d.identity.row,
      n_teams: d.identity.n_teams,
    });
  for (const [side, list] of [
    ["offense", d.offense],
    ["defense", d.defense],
  ] as const)
    for (const f of list ?? []) out.push({ section: `ff_${side}`, ...f });
  for (const r of d.players ?? []) out.push({ section: "key_player", ...r });
  if (d.scorer) out.push({ section: "top_scorer", ...d.scorer });
  if (d.lineups) {
    out.push({ section: "best_lineup", ...d.lineups.best });
    out.push({ section: "worst_lineup", ...d.lineups.worst });
  }
  for (const s of d.storylines) out.push({ section: "storyline", ...s });
  if (d.lastGame)
    out.push({
      section: "last_game",
      result: d.lastGame.result,
      context: d.lastGame.context,
      // For the Hebrew name (run.ts adds opp_team_name_he; display.inHebrew).
      opp_team_name: d.lastGame.row.opp_team_name,
    });
  return out;
}

/** For the chat only ("his net impact is #5 on the team"): each row's net
 * impact rank among its team's rows in the table the page shows, ties
 * sharing the better rank, and how many of the team's rows are ranked. The
 * model may not count rows itself, so the app supplies the rank. */
export function withTeamNetRank(rows: Cells[], col: string): Cells[] {
  const value = (r: Cells) => (r[col] == null || r[col] === "" ? NaN : Number(r[col]));
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
