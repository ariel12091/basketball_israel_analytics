import "server-only";
import { Deadline, withSnapshot, type Query } from "./db";
import { parseLineups } from "./lineupParams";
import { dbArgs, needsFilteredPath } from "./contextParams";
import { readTabReferences } from "./onoffRepo";
import { readVersion } from "./gameLogsRepo";
import { playerPopulation, type Identity } from "../rules/playerStatsTable";
import type { League, Row } from "../rules/gameLogs";
import { namedPicker, readPlayerNames } from "./playerNames";
const call = (name: string, types: string[]) =>
  `SELECT * FROM ${name}(${types.map((t, i) => `$${i + 1}::${t}`).join(",")})`;
export function parsePlayerStats(s: URLSearchParams, league: League) {
  const p = new URLSearchParams(s);
  p.delete("view");
  const parsed = parseLineups(p, league);
  parsed.c.starters = {
    offMin: null,
    offMax: null,
    defMin: null,
    defMax: null,
  };
  return parsed;
}
// helpers.R fetch_player_traditional_filtered: unchanged owners/signatures.
export async function readPlayerFacts(
  q: Query,
  p: ReturnType<typeof parsePlayerStats>,
) {
  const c = p.c,
    a = dbArgs(c),
    euro = c.league !== "il",
    comp = c.league === "eurocup" ? "U" : "E";
  if (!needsFilteredPath(c) && p.maxMargin == null)
    return q<Row>(
      `SELECT * FROM ${euro ? "euroleague" : "basketball_test"}.player_traditional_stats_mv WHERE ${euro ? "competition=$1::text AND game_year=$2::int4" : "game_year=$1::int4"}`,
      euro ? [comp, c.season] : [c.season],
    );
  const custom =
    p.maxMargin != null &&
    !(
      p.maxMargin === 5 &&
      p.marginStatus === "all" &&
      p.maxTime === 300 &&
      !p.otMargin
    );
  const context = [
    c.season,
    c.start,
    c.end,
    c.teams.length ? c.teams.join(",") : null,
    a.typeCsv,
    a.oppCsv,
    a.homeAway,
    a.outcome,
    a.rankSide,
    a.rankN,
    a.rankMetric,
  ];
  const clutch = [p.maxMargin, p.marginStatus, p.maxTime, p.otMargin],
    tail = [a.minGn, a.maxGn, a.lastN];
  const types = [
    "int4",
    "date",
    "date",
    ...Array(6).fill("text"),
    "int4",
    "text",
  ];
  if (!euro)
    return q<Row>(
      call(
        `basketball_test.get_player_traditional_${custom ? "custom_clutch" : "from_games"}`,
        [...types, "int4", "text", "int4", "bool", "int4", "int4", "int4"],
      ),
      [...context, ...clutch, ...tail],
    );
  return q<Row>(
    call(
      `euroleague.get_player_traditional_${custom ? "custom_clutch" : p.maxMargin != null ? "standard_clutch" : "pergame"}`,
      [
        "text",
        ...types,
        ...(custom ? ["int4", "text", "int4", "bool"] : []),
        "int4",
        "int4",
        "int4",
      ],
    ),
    [comp, ...context, ...(custom ? clutch : []), ...tail],
  );
}
async function read(q: Query, p: ReturnType<typeof parsePlayerStats>) {
  const c = p.c,
    euro = c.league !== "il",
    comp = c.league === "eurocup" ? "U" : "E";
  const raw = await readPlayerFacts(q, p);
  let identities = await q<Identity>(
    euro
      ? `SELECT DISTINCT fr.team_id,fr.player_id,fr.player_id::text AS identity_id,euroleague.person_display_name(p.display_name) AS display_name FROM euroleague.full_rosters fr JOIN euroleague.players p ON p.player_id=fr.player_id JOIN euroleague.schedule s ON s.game_id=fr.game_id WHERE s.competition=$1::text AND s.season=$2::int4 AND lower(p.provider_player_id) NOT IN ('team','total')`
      : `SELECT team_id,source_player_id AS player_id,identity_id::text AS identity_id,MIN(display_name) AS display_name FROM basketball_test.resolved_player_identity_v WHERE game_year=$1 GROUP BY team_id,source_player_id,identity_id`,
    euro ? [comp, c.season] : [c.season],
  );
  if (!euro) {
    identities = identities.filter(
      (r) =>
        Number.isFinite(Number(r.team_id)) &&
        r.team_id != null &&
        Number.isFinite(Number(r.player_id)) &&
        r.player_id != null &&
        r.identity_id != null,
    );
    const counts = new Map<string, number>();
    for (const r of identities) {
      const k = `${r.team_id}:${r.player_id}`;
      counts.set(k, (counts.get(k) ?? 0) + 1);
    }
    identities = identities.filter(
      (r) => counts.get(`${r.team_id}:${r.player_id}`) === 1,
    );
  }
  const players = await q<Row>(
    euro
      ? `SELECT DISTINCT fr.team_id,fr.player_id,euroleague.person_display_name(p.display_name) AS player_name FROM euroleague.full_rosters fr JOIN euroleague.players p ON p.player_id=fr.player_id JOIN euroleague.schedule s ON s.game_id=fr.game_id WHERE s.competition=$1::text AND s.season=$2::int4 AND lower(p.provider_player_id) NOT IN ('team','total') ORDER BY player_name`
      : `SELECT fr.team_id,fr.player_id,MIN(btrim(fr.team_name)) AS team_name,MIN(NULLIF(btrim(CONCAT_WS(' ',fr.firstname,fr.lastname)),'')) AS player_name FROM basketball_test.full_rosters fr WHERE fr.game_year=$1 AND fr.player_id IS NOT NULL AND fr.player_id>0 GROUP BY fr.team_id,fr.player_id HAVING MIN(NULLIF(btrim(CONCAT_WS(' ',fr.firstname,fr.lastname)),'')) IS NOT NULL ORDER BY player_name,team_name`,
    euro ? [comp, c.season] : [c.season],
  );
  return {
    rows: playerPopulation(raw, identities, c.teams),
    identities,
    players: namedPicker(
      players,
      await readPlayerNames(q, c.league, c.season),
      "player_name",
    ),
    meta: {
      ...(await readTabReferences(q, c)),
      version: await readVersion(q, c.league),
    },
  };
}
export type PlayerStatsPayload = Awaited<ReturnType<typeof read>>;
const cache = new Map<
  string,
  { expires: number; promise: Promise<PlayerStatsPayload> }
>();
export function playerStatsAnswer(league: League, s: URLSearchParams) {
  const p = parsePlayerStats(s, league),
    key = JSON.stringify(p),
    now = Date.now(),
    hit = cache.get(key);
  if (hit && hit.expires > now) return hit.promise;
  for (const [k, v] of cache) if (v.expires <= now) cache.delete(k);
  if (cache.size >= 16) cache.delete(cache.keys().next().value!);
  const promise = withSnapshot(new Deadline(25000, 20000), (q) => read(q, p));
  cache.set(key, { expires: now + 60000, promise });
  void promise.catch(() => {
    if (cache.get(key)?.promise === promise) cache.delete(key);
  });
  return promise;
}
