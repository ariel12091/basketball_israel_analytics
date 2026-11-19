import "server-only";
import { Deadline, withSnapshot, type Query } from "./db";
import type { League } from "../rules/gameLogs";
import { readVersion } from "./dataVersion";
import { VersionedCache } from "./versionedCache";
import type { TeamIdentity } from "../rules/teamColors";

/** Each team's logo and club colours, from its catalogue row
 * (docs/shared_definitions.md "Team logo", "Team colours"): Israeli
 * `season_teams` for the season, keyed by the roster team id; EuroLeague and
 * EuroCup `euroleague.teams` (one row per club, no season), keyed by its own
 * team id, which the schedule and the readers carry. A team with no row is
 * left out of the map. */
export async function readTeamIdentity(
  q: Query,
  league: string,
  season: number,
  teamIds: number[],
): Promise<Map<number, TeamIdentity>> {
  if (!teamIds.length) return new Map();
  const rows =
    league === "il"
      ? await q<Record<string, unknown>>(
          `SELECT team_id, logo_url AS logo, primary_color, secondary_color
           FROM basketball_test.season_teams
           WHERE game_year = $1::int4 AND team_id = ANY($2::int4[])`,
          [season, teamIds],
        )
      : await q<Record<string, unknown>>(
          `SELECT team_id, crest_url AS logo, primary_color, secondary_color
           FROM euroleague.teams
           WHERE team_id = ANY($1::int8[])`,
          [teamIds],
        );
  const text = (v: unknown) => (v == null || v === "" ? null : String(v));
  return new Map(
    rows.map((r) => [
      Number(r.team_id),
      { logo: text(r.logo), primary: text(r.primary_color), secondary: text(r.secondary_color) },
    ]),
  );
}

/** Every team's logo for one league-season ("Team logo"): the same catalogue
 * rows as readTeamIdentity, read once per data version for the tables. */
export type TeamLogos = Map<number, string>;
const LOGOS_SQL = {
  il: `SELECT team_id, logo_url AS logo FROM basketball_test.season_teams
       WHERE game_year = $1::int4 AND logo_url IS NOT NULL`,
  euro: `SELECT team_id, crest_url AS logo FROM euroleague.teams
         WHERE competition = $1 AND crest_url IS NOT NULL`,
};
const COMPETITION: Record<string, string> = { euroleague: "E", eurocup: "U" };
const logoCache = new VersionedCache<{ version: string; rows: [number, string][] }>(
  8,
  (v) => v.version,
);

export async function readTeamLogos(q: Query, league: string, season: number): Promise<TeamLogos> {
  const competition = COMPETITION[league];
  if ((league !== "il" && !competition) || !Number.isFinite(season)) return new Map();
  const version = await readVersion(q, league as League);
  const data = await logoCache.getAtVersion(league as League, version, String(season), async () => ({
    version,
    rows: (
      await q<Record<string, unknown>>(
        league === "il" ? LOGOS_SQL.il : LOGOS_SQL.euro,
        [league === "il" ? season : competition],
      )
    ).map((r): [number, string] => [Number(r.team_id), String(r.logo)]),
  }));
  return new Map(data.rows);
}

/** `rows` with `team_logo` (the row's team_id) and `opp_team_logo` (its
 * opp_team_id) where the catalogue has one; other cells are untouched. */
export function applyTeamLogos<T extends Record<string, unknown>>(rows: T[], logos: TeamLogos): T[] {
  if (!logos.size) return rows;
  const of = (id: unknown) => (id == null || id === "" ? undefined : logos.get(Number(id)));
  return rows.map((r) => {
    const own = of(r.team_id),
      opp = of(r.opp_team_id);
    if (!own && !opp) return r;
    return { ...r, ...(own ? { team_logo: own } : {}), ...(opp ? { opp_team_logo: opp } : {}) };
  });
}

/** Game Logs' season pull is cached outside any snapshot: the logos get their
 * own read; none when it fails. */
export async function loadTeamLogos(league: string, season: number): Promise<TeamLogos> {
  try {
    return await withSnapshot(new Deadline(), (q) => readTeamLogos(q, league, season));
  } catch {
    return new Map();
  }
}
