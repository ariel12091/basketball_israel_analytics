import "server-only";
import type { Query } from "./db";
import type { League, Row } from "../rules/gameLogs";
import { playerIds } from "../rules/lineups";
import { orderBySize } from "../rules/playerSize";
import { readVersion } from "./dataVersion";
import { VersionedCache } from "./versionedCache";

/** The "Player display name" owner (docs/shared_definitions.md, D27):
 * `player_display_names_v`, one English and one Hebrew name per (team, player)
 * -- Basket's, then the identity dictionary's, then the roster's. A reused
 * provider id (two people in one pair) has no name there and keeps the row's
 * own spelling. EuroLeague names come from `euroleague.players` through the
 * readers, so its entries carry no name (`en` null: nothing is renamed), only
 * the profile ("Player height, position and headshot",
 * `euroleague.player_season_profiles`, migration 067). */
export type PlayerName = {
  en: string | null;
  he: string | null;
  /** Height (m) and position code, for ordering lineups. */
  height?: number | null;
  position?: string | null;
  /** The player's headshot for that season and team. */
  photo?: string | null;
};
export type PlayerNames = Map<string, PlayerName>;
const key = (team: unknown, player: unknown) =>
  `${Number(team)}:${Number(player)}`;

// The view owns basket.co.il's height, position (2026-11-12,
// sql/migrations/2026-11-12_player_display_names_size.sql) and headshot
// (2026-11-19, sql/migrations/2026-11-19_basket_registration_photo.sql): the lineups'
// height order reads them here, as every other consumer will.
const SQL =
  "SELECT team_id,player_id,display_name AS name,display_name_he AS name_he,height_m,position_en,photo_url FROM basketball_test.player_display_names_v WHERE game_year=$1 AND display_name IS NOT NULL";
// EuroLeague and EuroCup: the season roster's profile per (team, player).
const EURO_SQL = `SELECT pr.team_id, pr.player_id, pr.height_m, pr.position_en, pr.headshot_url
FROM euroleague.player_season_profiles pr
JOIN euroleague.teams t ON t.team_id = pr.team_id
WHERE pr.season = $1 AND t.competition = $2`;
const COMPETITION: Record<string, string> = { euroleague: "E", eurocup: "U" };
const blank = (v: unknown) => v == null || String(v).trim() === "";
const text = (v: unknown) => (blank(v) ? null : String(v));
const cache = new VersionedCache<{
  version: string;
  names: [string, PlayerName][];
}>(8, (value) => value.version);

export async function readPlayerNames(
  q: Query,
  league: string,
  season: number,
): Promise<PlayerNames> {
  const competition = COMPETITION[league];
  if (league !== "il" && !competition) return new Map();
  const version = await readVersion(q, league === "il" ? "il" : "euroleague");
  const height = (v: unknown) => (v == null ? null : Number(v));
  const data = await cache.getAtVersion(
    league as League,
    version,
    String(season),
    async () => ({
      version,
      names:
        league === "il"
          ? (await q<Row>(SQL, [season]))
              .filter((r) => !blank(r.name))
              .map((r): [string, PlayerName] => [
                key(r.team_id, r.player_id),
                {
                  en: String(r.name),
                  he: text(r.name_he),
                  height: height(r.height_m),
                  position: text(r.position_en),
                  photo: text(r.photo_url),
                },
              ])
          : (await q<Row>(EURO_SQL, [season, competition])).map((r): [string, PlayerName] => [
              key(r.team_id, r.player_id),
              {
                en: null,
                he: null,
                height: height(r.height_m),
                position: text(r.position_en),
                photo: text(r.headshot_url),
              },
            ]),
    }),
  );
  return new Map(data.names);
}

/** The owner's name for one (team, player), if it has one. */
export function ownerName(
  names: PlayerNames,
  team: unknown,
  player: unknown,
): PlayerName | undefined {
  return names.get(key(team, player));
}

/** A picker list: rename, then restore the SQL's name order. Untouched
 * (no rename, no re-sort) when the league has no owner map. */
export function namedPicker<T extends Row>(
  rows: T[],
  names: PlayerNames,
  field: "name" | "player_name",
): T[] {
  // EuroLeague entries carry no names: the readers' own order stands.
  if (![...names.values()].some((n) => n.en)) return rows;
  return applyPlayerNames(rows, names).sort(
    (x, y) =>
      String(x[field]).localeCompare(String(y[field])) ||
      String(x.team_name ?? "").localeCompare(String(y.team_name ?? "")),
  );
}

/** Rewrite every player-name field a row carries with the owner's name, and
 * add the Hebrew name (`name_he`; `player_names_he_str` on lineup rows) for the
 * Ask chat. Rows without a (team_id, player_id) or an owner entry are returned
 * unchanged. Lineup rows rebuild `player_names` / `player_names_str` from
 * `player_ids` with the same ", " separator as `refresh_sub_lineups`, ordered
 * shortest to tallest (`orderBySize`; a lineup with no heights keeps the ids'
 * order); the Hebrew list, in the same order, is set only when every player
 * has one. */
export function applyPlayerNames<T extends Row>(
  rows: T[],
  names: PlayerNames,
): T[] {
  if (!names.size) return rows;
  return rows.map((r) => {
    const out: Row = { ...r };
    if (r.player_id != null) {
      const name = names.get(key(r.team_id, r.player_id));
      if (name?.en) {
        // On/Off rows carry first/last name; cleanDisplayNames keeps a set Player.
        if ("Player" in r || "First Name" in r || "firstname" in r)
          out.Player = name.en;
        if ("player_name" in r) out.player_name = name.en;
        if ("name" in r) out.name = name.en;
      }
      if (name?.he) out.name_he = name.he;
      if (name?.photo) out.player_photo = name.photo;
    }
    if (r.player_names_str != null && r.player_ids != null) {
      const ids = playerIds(r.player_ids),
        // EuroLeague rows carry only the string, in player_ids order, and its
        // names ("FIRST LAST") hold no comma.
        old = Array.isArray(r.player_names)
          ? r.player_names
          : String(r.player_names_str).split(/\s*,\s*/);
      // Shortest to tallest, as Gameflow lists a lineup ("Lineup name order").
      const members = orderBySize(
        ids.map((id, i) => {
          const n = names.get(key(r.team_id, id));
          return { n, en: n?.en ?? String(old[i] ?? `#${id}`) };
        }),
        (m) => (m.n ? { height: m.n.height ?? null, position: m.n.position ?? null } : undefined),
        (m) => m.en,
      );
      const owned = members.map((m) => m.n);
      const list = members.map((m) => m.en);
      // Row types scalars only; the readers return this column as an array.
      (out as Record<string, unknown>).player_names = list;
      out.player_names_str = list.join(", ");
      // rankLineups copies the names into the table's Players column first.
      if ("Players" in r) out.Players = out.player_names_str;
      if (owned.every((n) => n?.he)) out.player_names_he_str = owned.map((n) => n!.he).join(", ");
    }
    return out as T;
  });
}
