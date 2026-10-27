import "server-only";
import type { Query } from "./db";
import type { Row } from "../rules/gameLogs";
import { playerIds } from "../rules/lineups";
import { readVersion } from "./dataVersion";
import { VersionedCache } from "./versionedCache";

/** The "Player display name" owner (docs/shared_definitions.md, D27):
 * `display_name` from `resolved_player_identity_v` per (team, player). The view
 * already falls back to the roster spelling where the dictionary has no entry.
 * Israeli league only; EuroLeague names come from `euroleague.players`. */
export type PlayerNames = Map<string, string>;
const key = (team: unknown, player: unknown) =>
  `${Number(team)}:${Number(player)}`;

// The provider recycles ids, so one (team, player) can be two people in a
// season (2026 team 5, player 2060). Such a pair keeps the row's own spelling,
// as Player Stats drops it from its identity join.
const SQL =
  "SELECT team_id,source_player_id AS player_id,MIN(display_name) AS name FROM basketball_test.resolved_player_identity_v WHERE game_year=$1 AND source_player_id IS NOT NULL GROUP BY team_id,source_player_id HAVING COUNT(DISTINCT display_name)=1";
const cache = new VersionedCache<{
  version: string;
  names: [string, string][];
}>(4, (value) => value.version);

export async function readPlayerNames(
  q: Query,
  league: string,
  season: number,
): Promise<PlayerNames> {
  if (league !== "il") return Promise.resolve(new Map());
  const version = await readVersion(q, "il");
  const data = await cache.getAtVersion(
    "il",
    version,
    String(season),
    async () => ({
      version,
      names: (await q<Row>(SQL, [season]))
        .filter((r) => r.name != null && String(r.name).trim() !== "")
        .map((r): [string, string] => [
          key(r.team_id, r.player_id),
          String(r.name),
        ]),
    }),
  );
  return new Map(data.names);
}

/** A picker list: rename, then restore the SQL's name order. Untouched
 * (no rename, no re-sort) when the league has no owner map. */
export function namedPicker<T extends Row>(
  rows: T[],
  names: PlayerNames,
  field: "name" | "player_name",
): T[] {
  if (!names.size) return rows;
  return applyPlayerNames(rows, names).sort(
    (x, y) =>
      String(x[field]).localeCompare(String(y[field])) ||
      String(x.team_name ?? "").localeCompare(String(y.team_name ?? "")),
  );
}

/** Rewrite every player-name field a row carries with the owner's name. Rows
 * without a (team_id, player_id) or an owner entry are returned unchanged.
 * Lineup rows rebuild `player_names` / `player_names_str` from `player_ids`,
 * in the same order and with the same ", " separator as `refresh_sub_lineups`. */
export function applyPlayerNames<T extends Row>(
  rows: T[],
  names: PlayerNames,
): T[] {
  if (!names.size) return rows;
  return rows.map((r) => {
    const out: Row = { ...r };
    if (r.player_id != null) {
      const name = names.get(key(r.team_id, r.player_id));
      if (name) {
        // On/Off rows carry first/last name; cleanDisplayNames keeps a set Player.
        if ("Player" in r || "First Name" in r || "firstname" in r)
          out.Player = name;
        if ("player_name" in r) out.player_name = name;
        if ("name" in r) out.name = name;
      }
    }
    if (r.player_names_str != null && r.player_ids != null) {
      const ids = playerIds(r.player_ids),
        old = Array.isArray(r.player_names) ? r.player_names : [];
      const list = ids.map(
        (id, i) => names.get(key(r.team_id, id)) ?? String(old[i] ?? `#${id}`),
      );
      // Row types scalars only; the readers return this column as an array.
      (out as Record<string, unknown>).player_names = list;
      out.player_names_str = list.join(", ");
    }
    return out as T;
  });
}
