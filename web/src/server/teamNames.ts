import "server-only";
import { Deadline, withSnapshot, type Query } from "./db";
import type { Row } from "../rules/gameLogs";
import { readVersion } from "./dataVersion";
import { VersionedCache } from "./versionedCache";
import { applyTeamLogos, readTeamLogos } from "./teamIdentity";

/** Hebrew team names in the Israeli answers (team_name_he, opp_team_name_he):
 * basket.co.il's spelling, stored per season in
 * `schedule_team_dict.team_name_basket` (the source the player names already
 * use, D27). A workaround until `season_teams` carries a Hebrew display name;
 * no page shows these; the API returns them and the Ask chat quotes them.
 *
 * A row is matched by its `team_id` (the roster id) when it has one, and
 * otherwise by an English name cell -- Gameflow and Game Logs carry names
 * only -- in any of the three spellings a reader may return (the catalogue's,
 * the schedule's, the rosters').
 *
 * The English name shown is the catalogue's (`season_teams`, basket.co.il's
 * schedule spelling, rule 2): readers built on the rosters return the
 * provider's spelling ("IRONI EILAT" for "HAPOEL EILAT"), so every English
 * team-name cell that holds any known spelling is rewritten to it (`byNameEn`),
 * and a row with a `team_id` gets its team's name in `team_name` / `Team`
 * whatever spelling it held (`byIdEn`: "KIRYAT ATA", a spelling none of the
 * three tables records, is "IRONI KIRYAT ATA"). */
export type TeamNamesHe = {
  byId: Map<number, string>;
  byName: Map<string, string>;
  byNameEn: Map<string, string>;
  byIdEn: Map<number, string>;
};

const SQL = `SELECT d.team_id_rosters AS team_id, d.team_name_basket AS name_he,
  t.team_name, d.team_name_schedule, d.team_name_rosters
FROM basketball_test.schedule_team_dict d
LEFT JOIN basketball_test.season_teams t
  ON t.game_year = d.game_year AND t.team_id = d.team_id_rosters
WHERE d.game_year = $1 AND d.team_name_basket IS NOT NULL`;

/** Case and punctuation do not tell spellings apart: final_schedule_mv has
 * both "HAPOEL TEL AVIV" and "Hapoel Tel-Aviv" in 2027. */
const norm = (v: unknown) =>
  String(v ?? "")
    .toUpperCase()
    .replace(/[^\p{L}\p{N}]+/gu, " ")
    .trim();
const cache = new VersionedCache<{ version: string; rows: Row[] }>(4, (v) => v.version);

export function teamNamesFromRows(rows: Row[]): TeamNamesHe {
  const byId = new Map<number, string>(),
    byName = new Map<string, string>(),
    byNameEn = new Map<string, string>(),
    byIdEn = new Map<number, string>();
  for (const r of rows) {
    const en = String(r.team_name ?? "").trim();
    if (en) byIdEn.set(Number(r.team_id), en);
    if (en)
      for (const k of ["team_name", "team_name_schedule", "team_name_rosters"]) {
        const n = norm(r[k]);
        if (n) byNameEn.set(n, en);
      }
    const he = String(r.name_he ?? "").trim();
    if (!he) continue;
    byId.set(Number(r.team_id), he);
    for (const k of ["team_name", "team_name_schedule", "team_name_rosters"]) {
      const n = norm(r[k]);
      if (n) byName.set(n, he);
    }
  }
  return { byId, byName, byNameEn, byIdEn };
}

async function read(q: Query, season: number): Promise<TeamNamesHe> {
  const version = await readVersion(q, "il");
  const data = await cache.getAtVersion("il", version, String(season), async () => ({
    version,
    rows: await q<Row>(SQL, [season]),
  }));
  return teamNamesFromRows(data.rows);
}

const empty = (): TeamNamesHe => ({
  byId: new Map(),
  byName: new Map(),
  byNameEn: new Map(),
  byIdEn: new Map(),
});

/** The season's Hebrew team names, read in the request's own snapshot (the
 * answer functions call this, as they call readPlayerNames); empty outside
 * the Israeli league. */
export async function readTeamNamesHe(q: Query, league: string, season: number): Promise<TeamNamesHe> {
  if (league !== "il" || !Number.isFinite(season)) return empty();
  return read(q, season);
}

/** `rows` with each team's logo (`team_logo`, `opp_team_logo`; every league,
 * server/teamIdentity.ts) and team_name_he / opp_team_name_he (Israeli league
 * only): the one call each answer function makes. */
export async function decorateTeams<T extends Record<string, unknown>>(
  q: Query,
  league: string,
  season: number,
  rows: T[],
): Promise<T[]> {
  // The rows' own type: the added string fields fit any record of cells.
  const withLogos = applyTeamLogos(rows, await readTeamLogos(q, league, season));
  if (league !== "il") return withLogos;
  return applyTeamNamesHe(withLogos, await readTeamNamesHe(q, league, season)) as T[];
}

/** The chat's team list (list_teams has no answer function): its own
 * snapshot; empty when the read fails, so teams are named in English. */
export async function loadTeamNamesHe(league: string, season: number): Promise<TeamNamesHe> {
  if (league !== "il" || !Number.isFinite(season)) return empty();
  try {
    return await withSnapshot(new Deadline(), (q) => read(q, season));
  } catch {
    return empty();
  }
}

/** One English team name's Hebrew name, if known (any spelling). */
export const teamNameHe = (names: TeamNamesHe, name: unknown): string | undefined =>
  names.byName.get(norm(name));

/** The English name cells a row may carry for its own team. */
const OWN = ["team_name", "Team", "team", "entity_name", "name"] as const;

/** Add `team_name_he` (the row's team) and `opp_team_name_he` (Game Logs'
 * opponent) where a Hebrew name is known, and rewrite each English team-name
 * cell (`ownNameCells`, `opp_team_name`) that holds a known spelling to the
 * catalogue's; other cells are untouched. A player's `name` (lineup_roster)
 * never equals a team name, so it is never matched. */
type WithHe<T> = T & { team_name_he?: string; opp_team_name_he?: string };
export function applyTeamNamesHe<T extends Record<string, unknown>>(
  rows: T[],
  names: TeamNamesHe,
  ownNameCells: readonly string[] = OWN,
): WithHe<T>[] {
  if (!names.byId.size && !names.byName.size && !names.byNameEn.size) return rows;
  const enCells = [...ownNameCells, "opp_team_name"];
  return rows.map((r): WithHe<T> => {
    const en: Record<string, string> = {};
    for (const k of enCells) {
      const v = r[k];
      const shown = typeof v === "string" ? names.byNameEn.get(norm(v)) : undefined;
      if (shown && shown !== v) en[k] = shown;
    }
    // By id: the row's own team cells, unless it is a TOTAL or multi-team row.
    const byIdName =
      r.team_id != null && r.team_id !== "" && !r.is_multi_team_total
        ? names.byIdEn.get(Number(r.team_id))
        : undefined;
    if (byIdName)
      for (const k of ["team_name", "Team"]) {
        const v = r[k];
        if (typeof v === "string" && v && norm(v) !== "TOTAL" && v !== byIdName)
          en[k] = byIdName;
      }
    let he: string | undefined =
      r.team_id != null && r.team_id !== "" ? names.byId.get(Number(r.team_id)) : undefined;
    if (!he)
      for (const k of ownNameCells) {
        he = names.byName.get(norm(r[k]));
        if (he) break;
      }
    const opp = r.opp_team_name != null ? names.byName.get(norm(r.opp_team_name)) : undefined;
    if (!he && !opp && !Object.keys(en).length) return r;
    const out: WithHe<T> = { ...r, ...en };
    if (he) out.team_name_he = he;
    if (opp) out.opp_team_name_he = opp;
    return out;
  });
}
