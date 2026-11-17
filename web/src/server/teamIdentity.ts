import "server-only";
import type { Query } from "./db";
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
