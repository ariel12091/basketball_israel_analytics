import "server-only";
import { Deadline, withSnapshot, type Query } from "./db";
import type { Row } from "../rules/gameLogs";
import { readVersion } from "./dataVersion";
import { VersionedCache } from "./versionedCache";

/** Hebrew team names for the Ask chat only: basket.co.il's spelling, stored
 * per season in `schedule_team_dict.team_name_basket` (the source the player
 * names already use, D27). A workaround until `season_teams` carries a Hebrew
 * display name; no page shows these. Israeli league only.
 *
 * A row is matched by its `team_id` (the roster id) when it has one, and
 * otherwise by an English name cell -- Gameflow and Game Logs carry names
 * only -- in any of the three spellings a reader may return (the catalogue's,
 * the schedule's, the rosters'). */
export type TeamNamesHe = { byId: Map<number, string>; byName: Map<string, string> };

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
    byName = new Map<string, string>();
  for (const r of rows) {
    const he = String(r.name_he ?? "").trim();
    if (!he) continue;
    byId.set(Number(r.team_id), he);
    for (const k of ["team_name", "team_name_schedule", "team_name_rosters"]) {
      const n = norm(r[k]);
      if (n) byName.set(n, he);
    }
  }
  return { byId, byName };
}

async function read(q: Query, season: number): Promise<TeamNamesHe> {
  const version = await readVersion(q, "il");
  const data = await cache.getAtVersion("il", version, String(season), async () => ({
    version,
    rows: await q<Row>(SQL, [season]),
  }));
  return teamNamesFromRows(data.rows);
}

/** The season's Hebrew team names; empty outside the Israeli league or when
 * the read fails (the chat then names teams in English, as before). */
export async function readTeamNamesHe(league: string, season: number): Promise<TeamNamesHe> {
  const empty = { byId: new Map(), byName: new Map() };
  if (league !== "il" || !Number.isFinite(season)) return empty;
  try {
    return await withSnapshot(new Deadline(), (q) => read(q, season));
  } catch {
    return empty;
  }
}

/** The English name cells a row may carry for its own team. */
const OWN = ["team_name", "Team", "team", "entity_name", "name"] as const;

/** Add `team_name_he` (the row's team) and `opp_team_name_he` (Game Logs'
 * opponent) where a Hebrew name is known; other cells are untouched. A
 * player's `name` (lineup_roster) never equals a team name, so it adds
 * nothing by name. */
type WithHe<T> = T & { team_name_he?: string; opp_team_name_he?: string };
export function applyTeamNamesHe<T extends Record<string, unknown>>(
  rows: T[],
  names: TeamNamesHe,
  ownNameCells: readonly string[] = OWN,
): WithHe<T>[] {
  if (!names.byId.size && !names.byName.size) return rows;
  return rows.map((r): WithHe<T> => {
    let he: string | undefined =
      r.team_id != null && r.team_id !== "" ? names.byId.get(Number(r.team_id)) : undefined;
    if (!he)
      for (const k of ownNameCells) {
        he = names.byName.get(norm(r[k]));
        if (he) break;
      }
    const opp = r.opp_team_name != null ? names.byName.get(norm(r.opp_team_name)) : undefined;
    if (!he && !opp) return r;
    const out: WithHe<T> = { ...r };
    if (he) out.team_name_he = he;
    if (opp) out.opp_team_name_he = opp;
    return out;
  });
}
