// Each answer function's payload as plain rows for the ask tools. No values
// are derived here except through the pages' own rules (rules/home.ts).
import {
  homeDashboard,
  homeRank,
  type HomeFrames,
} from "@/rules/home";
import type { ToolName } from "./registry";

type Cells = Record<string, unknown>;
export type Flat = { rows: Cells[]; total: Cells | null };

export function flatten(
  tool: ToolName,
  payload: unknown,
  query: URLSearchParams,
): Flat {
  const p = (payload ?? {}) as { rows?: Cells[]; frames?: HomeFrames };
  if (tool === "gameflow") return { rows: [], total: null };
  if (tool === "team_overview")
    return { rows: overview(p.frames, query.get("team_id") ?? ""), total: null };
  const rows = p.rows ?? [];
  if (tool === "lineups") {
    // LineupTable: the is_total === 0 row is the pinned "- All Lineups -" TOTAL.
    const total = rows.find((r) => r.is_total === 0) ?? null;
    return { rows: rows.filter((r) => r !== total), total };
  }
  return { rows, total: null };
}

/** The Home hub's sections (homeDashboard) as one row each. */
function overview(frames: HomeFrames | undefined, team: string): Cells[] {
  if (!frames || !team) return [];
  const d = homeDashboard(frames, team);
  const out: Cells[] = [];
  if (d.identity)
    out.push({
      section: "identity",
      ...d.identity.row,
      n_teams: d.identity.n_teams,
      off_rank: homeRank(frames.ratings, team, "off_ppp", true),
      def_rank: homeRank(frames.ratings, team, "def_ppp", false),
      net_rank: homeRank(frames.ratings, team, "net_rtg", true),
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
