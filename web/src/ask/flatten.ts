// Each answer function's payload as plain rows for the ask tools. No values
// are derived here except through the pages' own rules (rules/home.ts).
import { homeDashboard, type HomeFrames } from "@/rules/home";
import type { League } from "@/rules/gameLogs";
import { pageRows } from "./pageRows";
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
  const rows: Cells[] = pageRows(tool, league, payload, query);
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
    players?: PlayerRating[];
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
    });
  return out;
}
