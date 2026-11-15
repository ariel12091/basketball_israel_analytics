// The Player Stats table's page state: what PlayerStatsClient, the API's
// shape=table (playerStatsPageAnswer) and the chat pass to playerDisplay().
import type { Row } from "../../rules/gameLogs";
import {
  PLAYER_MODES,
  playerDisplay,
  type Identity,
  type PlayerMode,
} from "../../rules/playerStatsTable";
import { parseStats, type Query } from "../tabQuery";

const list = (v: string | undefined) => (v ?? "").split(",").filter(Boolean);

/** shape=table: the answer with its rows replaced by the page's table. */
export function playerStatsTable<
  A extends { rows: Row[]; identities?: unknown[]; meta: object },
>(answer: A, query: Query) {
  const rows = playerDisplay(
    answer.rows,
    (answer.identities ?? []) as Identity[],
    playerStatsPageOptions(query, answer.rows),
  ).rows;
  return { ...answer, rows, meta: { ...answer.meta, shape: "table" as const } };
}

export function playerStatsPageOptions(query: Query, rows: Row[]) {
  const mode = (
    PLAYER_MODES.includes(query.view as PlayerMode) ? query.view : "Per Game"
  ) as PlayerMode;
  // A minimum above every player's games would empty the table.
  const maxGp = Math.max(1, ...rows.map((r) => Number(r.gp) || 0));
  return {
    /** The min_gp stepper's ceiling; playerDisplay ignores it. */
    maxGp,
    mode,
    players: list(query.players),
    minGp: Math.min(maxGp, Math.max(1, Math.trunc(Number(query.min_gp) || 1))),
    showIneligible: query.show_ineligible === "true",
    filters: parseStats(query.stats ?? ""),
  };
}
