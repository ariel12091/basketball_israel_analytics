import "server-only";
import { withSnapshot, Deadline, type Query } from "./db";
import { dbArgs, needsFilteredPath } from "./contextParams";
import { readVersion } from "./gameLogsRepo";
import { readTabReferences } from "./onoffRepo";
import { applyPlayerNames, namedPicker, readPlayerNames } from "./playerNames";
import type { LineupParams } from "./lineupParams";
import type { Row } from "../rules/gameLogs";
import { euroLineupRates, rankLineups } from "../rules/lineups";
import ui from "../data/shinyUi.json";

const call = (name: string, types: string[]) =>
  `SELECT * FROM ${name}(${types.map((t, i) => `$${i + 1}::${t}`).join(",")})`;
const IL_TYPES = [
  "int4",
  "text",
  "text",
  "text",
  "bool",
  "date",
  "date",
  "int4",
  "int4",
  "text",
  "text",
  "text",
  "text",
  "text",
  "int4",
  "text",
  "int4",
  "text",
  "int4",
  "bool",
  ...Array(9).fill("int4"),
];
const EL_HEAD = [
  "text",
  "int4",
  "date",
  "date",
  ...Array(6).fill("text"),
  "int4",
  "text",
];
const EL_TAIL = [...Array(8).fill("int4"), "text", "text", "int4"];
/** server_tab2 run_fetch_lineups_*; server_tab10's unchanged three readers.
 * Fetch the complete size/context population, then rank before local filters. */
async function read(q: Query, p: LineupParams) {
  const { c, size, view } = p,
    euro = c.league !== "il",
    comp = c.league === "eurocup" ? "U" : "E",
    a = dbArgs(c);
  const version = await readVersion(q, c.league);
  const filtered = needsFilteredPath(c) || p.maxMargin != null;
  let rows: Row[];
  if (!euro) {
    rows = await q<Row>(
      call(
        `basketball_test.${view === "four_factors" ? "fetch_lineups_four_factors_csv" : "fetch_lineups_csv_v2"}`,
        IL_TYPES,
      ),
      [
        size,
        null,
        null,
        null,
        true,
        c.start,
        c.end,
        0,
        c.season,
        a.typeCsv,
        a.oppCsv,
        a.homeAway,
        a.outcome,
        a.rankSide,
        a.rankN,
        a.rankMetric,
        p.maxMargin,
        p.marginStatus,
        p.maxTime,
        p.otMargin,
        a.minGn,
        a.maxGn,
        a.lastN,
        null,
        null,
        a.offMin,
        a.offMax,
        a.defMin,
        a.defMax,
      ],
    );
  } else if (!filtered) {
    rows = await q<Row>(
      "SELECT * FROM euroleague.sub_lineups_stats_mv WHERE competition=$1::text AND game_year=$2::int4 AND unit_size=$3::int4",
      [comp, c.season, size],
    );
  } else {
    const kind =
      p.maxMargin == null &&
      p.maxTime == null &&
      (!p.marginStatus || p.marginStatus === "all")
        ? "pergame"
        : p.maxMargin === 5 &&
            p.maxTime === 300 &&
            p.marginStatus === "all" &&
            !p.otMargin
          ? "dynamic"
          : "direct";
    const head = [
      comp,
      c.season,
      c.start,
      c.end,
      null,
      a.typeCsv,
      a.oppCsv,
      a.homeAway,
      a.outcome,
      a.rankSide,
      a.rankN,
      a.rankMetric,
    ];
    const tail = [
      a.minGn,
      a.maxGn,
      a.lastN,
      a.offMin,
      a.offMax,
      a.defMin,
      a.defMax,
      size,
      null,
      null,
      0,
    ];
    const extra =
      kind === "pergame"
        ? []
        : [p.maxMargin, p.marginStatus, p.maxTime, p.otMargin];
    rows = await q<Row>(
      call(`euroleague.fetch_lineups_${kind}`, [
        ...EL_HEAD,
        ...(kind === "pergame" ? [] : ["int4", "text", "int4", "bool"]),
        ...EL_TAIL,
      ]),
      [...head, ...extra, ...tail],
    );
  }
  // The Euro filtered reader returns player_ids as a PostgreSQL array; preserve
  // it for the same local membership predicate as the season view.
  rows = rankLineups(euro ? euroLineupRates(rows) : rows, euro);
  rows = applyPlayerNames(rows, await readPlayerNames(q, c.league, c.season));
  const meta = await readTabReferences(q, c);
  const teams = euro
    ? meta.teams
    : ((ui.ilTeams as Record<string, { id: number; name: string }[]>)[
        String(c.season)
      ] ?? []);
  const names = new Map(teams.map((t) => [String(t.id), t.name]));
  return {
    rows: rows.map((r) => ({
      ...r,
      Team: names.get(String(r.team_id)) ?? String(r.team_id),
    })),
    meta: {
      ...meta,
      teams,
      version,
      view,
      size,
      league: c.league,
      season: c.season,
      filtered,
    },
  };
}
const cache = new Map<
  string,
  { expires: number; promise: ReturnType<typeof read> }
>();
export function getLineups(p: LineupParams) {
  const key = JSON.stringify({ ...p, c: { ...p.c, teams: [] } }),
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
/** fetch_players_basic/euro_fetch_players_basic + with_season_minutes. */
export function getLineupRoster(p: LineupParams) {
  const { c } = p,
    euro = c.league !== "il",
    comp = c.league === "eurocup" ? "U" : "E";
  return withSnapshot(new Deadline(25000, 20000), async (q) => {
    const players = await q<Row>(
      euro
        ? "SELECT DISTINCT fr.player_id,p.display_name AS name,fr.team_id FROM euroleague.full_rosters fr JOIN euroleague.players p ON p.player_id=fr.player_id JOIN euroleague.schedule s ON s.game_id=fr.game_id WHERE s.competition=$1::text AND s.season=$2::int4 AND lower(p.provider_player_id) NOT IN ('team','total') ORDER BY p.display_name"
        : "SELECT team_id,player_id,MIN(btrim(firstname)||' '||btrim(lastname)) AS name FROM basketball_test.full_rosters WHERE game_year=$1::int4 GROUP BY team_id,player_id ORDER BY MIN(btrim(firstname)||' '||btrim(lastname))",
      euro ? [comp, c.season] : [c.season],
    );
    const mins = await q<Row>(
      euro
        ? "SELECT team_id,player_id,minutes FROM euroleague.player_traditional_stats_mv WHERE competition=$1::text AND game_year=$2::int4"
        : "SELECT team_id,player_id,minutes FROM basketball_test.player_traditional_stats_mv WHERE game_year=$1::int4",
      euro ? [comp, c.season] : [c.season],
    );
    const sums = new Map<string, number>();
    for (const r of mins) {
      const k = `${r.team_id}|${r.player_id}`;
      sums.set(k, (sums.get(k) ?? 0) + Number(r.minutes ?? 0));
    }
    const named = namedPicker(
      players,
      await readPlayerNames(q, c.league, c.season),
      "name",
    );
    return {
      rows: named.map((r) => ({
        ...r,
        minutes: sums.get(`${r.team_id}|${r.player_id}`) ?? null,
      })),
      meta: { version: await readVersion(q, c.league) },
    };
  });
}
