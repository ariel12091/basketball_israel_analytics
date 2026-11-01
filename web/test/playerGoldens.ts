import { existsSync, readdirSync, readFileSync } from "node:fs";
import type { League, Row } from "../src/rules/gameLogs";
import type { PlayerStatsPayload } from "../src/server/playerStatsRepo";
import {
  playerDisplay,
  type PlayerMode,
  PLAYER_LABELS,
} from "../src/rules/playerStatsTable";
import { parseStats } from "../src/lib/tabQuery";
export const PLAYER_DIR = "test/goldens/player_stats";
export const playerPresets = existsSync(PLAYER_DIR)
  ? readdirSync(PLAYER_DIR).filter(
      (f) =>
        f.endsWith(".json") &&
        !f.endsWith(".answer.json") &&
        !f.endsWith(".names.json"),
    )
  : [];
export const PLAYER_ORACLE_MAP: Record<string, string> = {
  Team: "team_name",
  Player: "Player",
  ...Object.fromEntries(Object.entries(PLAYER_LABELS).map(([k, v]) => [v, k])),
  "Poss/G": "base_poss_pg",
  "Min/G": "base_min_pg",
  ".eligible_rate": "rate_eligible",
  ".is_total": "is_multi_team_total",
};
/** The stored answers predate names following the display-name owner (D27).
 * Rename them with the recorded owner names (`display_names_<season>.names.json`,
 * from scripts/export_player_goldens_json.R) as the code now does:
 * playerStatsRepo renames the identities, playerPopulation names each
 * per-team row from them and a multi-team player after his first row. */
function withOwnerNames(
  answer: PlayerStatsPayload,
  league: League,
  season: string | undefined,
): PlayerStatsPayload {
  const file = `${PLAYER_DIR}/display_names_${season}.names.json`;
  if (league !== "il" || !existsSync(file)) return answer;
  const key = (r: { team_id?: unknown; player_id?: unknown }) =>
    `${Number(r.team_id)}:${Number(r.player_id)}`;
  const owner = new Map(
    (JSON.parse(readFileSync(file, "utf8")) as Row[]).map((r) => [
      key(r),
      String(r.display_name),
    ]),
  );
  const identities = answer.identities.map((r) => ({
    ...r,
    display_name: owner.get(key(r)) ?? r.display_name,
  }));
  const byKey = new Map(identities.map((r) => [key(r), r]));
  let rows = answer.rows.map((r): Row =>
    r.is_multi_team_total
      ? { ...r }
      : { ...r, Player: byKey.get(key(r))?.display_name || r.Player },
  );
  const multi = new Set(
    rows.filter((r) => r.is_multi_team_total).map((r) => String(r[".identity_id"])),
  );
  const first = new Map<string, Row["Player"]>();
  for (const r of rows) {
    const id = String(r[".identity_id"]);
    if (!r.is_multi_team_total && multi.has(id) && !first.has(id))
      first.set(id, r.Player);
  }
  rows = rows.map((r) =>
    multi.has(String(r[".identity_id"]))
      ? { ...r, Player: first.get(String(r[".identity_id"])) ?? r.Player }
      : r,
  );
  return { ...answer, identities, rows };
}

export function loadPlayer(file: string) {
  const g = JSON.parse(readFileSync(`${PLAYER_DIR}/${file}`, "utf8")) as {
    id: string;
    league: League;
    view: PlayerMode;
    query: Record<string, string>;
    expected: { data: Row[]; hidden: string[] };
  };
  const answer = withOwnerNames(
    JSON.parse(
      readFileSync(`${PLAYER_DIR}/${g.id}.answer.json`, "utf8"),
    ) as PlayerStatsPayload,
    g.league,
    g.query.season,
  );
  const result = playerDisplay(answer.rows, answer.identities, {
    mode: g.view,
    minGp: Number(g.query.min_gp ?? 1),
    players: (g.query.players ?? "").split(",").filter(Boolean),
    showIneligible: g.query.show_ineligible === "true",
    filters: parseStats(g.query.stats ?? ""),
  });
  return { g, answer, ...result };
}
