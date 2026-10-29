import "server-only";
import type { Row } from "../rules/gameLogs";
import { withSnapshot, Deadline, type Query } from "./db";
import { parseLineups } from "./lineupParams";
import { dbArgs, seasonBounds } from "./contextParams";
import { ParamError } from "./api";
import { readVersion } from "./gameLogsRepo";
import { VersionedCache } from "./versionedCache";
import { applyPlayerNames, namedPicker, readPlayerNames } from "./playerNames";
import { addUsagePct, normalizeTsRows } from "../rules/playerStats";
import { playerIds } from "../rules/lineups";
import data from "../data/compareUi.json";
import ui from "../data/shinyUi.json";
const KINDS = [
  "refs",
  "player_defaults",
  "ratings",
  "four_factors",
  "lineups",
  "lineup_ff",
  "shooting",
  "players",
  "player_ff",
  "onoff",
];
export function parseCompare(s: URLSearchParams) {
  const kind = s.get("kind") ?? "ratings";
  if (!KINDS.includes(kind))
    throw new ParamError("kind", "Invalid Compare reader");
  const p = parseLineups(s, "il");
  // Compare's collect_side_params() supplies both endpoints of a starter
  // bound. Preserve that signature, including the preset-cache classifier.
  for (const side of ["off", "def"] as const) {
    const min = `${side}Min` as const,
      max = `${side}Max` as const;
    if (p.c.starters[min] != null || p.c.starters[max] != null) {
      p.c.starters[min] ??= 0;
      p.c.starters[max] ??= 5;
    }
  }
  const ids = (key: string) => {
    const value = s.get(key) ?? "";
    if (value && !/^\d+(,\d+)*$/.test(value))
      throw new ParamError(key, "Invalid IDs");
    const a = value.split(",").filter(Boolean).map(Number);
    if (a.length > 50 || a.some((x) => x < 1 || x > 2147483647))
      throw new ParamError(key, "Invalid IDs");
    return a;
  };
  return {
    ...p,
    kind,
    on: ids("players_on"),
    any: ids("players_any"),
    off: ids("players_off"),
    empty: s.get("empty") === "true",
  };
}
type Params = ReturnType<typeof parseCompare>;
export function compareDynamicArgs(p: Params) {
  const c = p.c,
    a = dbArgs(c);
  return [
    c.season,
    c.start,
    c.end,
    a.typeCsv,
    a.oppCsv,
    a.homeAway,
    a.outcome,
    a.rankSide,
    a.rankN,
    a.rankMetric,
    p.maxMargin,
    null,
    p.maxTime,
    false,
    a.minGn,
    a.maxGn,
    null,
    null,
    null,
    a.offMin,
    a.offMax,
    a.defMin,
    a.defMax,
  ];
}
export function compareLineupArgs(p: Params) {
  const d = compareDynamicArgs(p),
    c = p.c;
  return [
    p.kind === "shooting" ? 5 : p.size,
    c.teams.length ? c.teams.join(",") : null,
    p.any.length ? null : p.on.join(",") || null,
    p.off.join(",") || null,
    !p.any.length && p.on.length > 0,
    c.start,
    c.end,
    0,
    c.season,
    ...d.slice(3),
  ];
}
const call = (name: string, types: string[]) =>
  `SELECT * FROM basketball_test.${name}(${types.map((t, i) => `$${i + 1}::${t}`).join(",")})`;
const playerTypes = [
  "int4",
  "date",
  "date",
  ...Array(6).fill("text"),
  "int4",
  "text",
  "int4",
  "text",
  "int4",
  "bool",
  "int4",
  "int4",
  "int4",
];
export function presetVariant(p: Params): string | null {
  const c = p.c,
    b = seasonBounds("il", c.season),
    a = dbArgs(c);
  if (
    c.start !== b.start ||
    c.end !== b.end ||
    c.gameTypes.length ||
    c.opponents.length ||
    c.homeAway ||
    c.outcome ||
    c.minGn != null ||
    c.maxGn != null ||
    c.lastN != null ||
    a.defMin != null ||
    a.defMax != null
  )
    return null;
  const noRank =
      !c.rankSide &&
      c.rankN == null &&
      (!c.rankMetric || c.rankMetric === "net"),
    noSt = a.offMin == null && a.offMax == null;
  if (noRank && p.maxMargin == null && noSt) return "overall";
  if (noRank && p.maxMargin == null && a.offMin === 3 && a.offMax === 5)
    return "starters_hi";
  if (noRank && p.maxMargin == null && a.offMin === 0 && a.offMax === 2)
    return "starters_lo";
  if (noRank && noSt && p.maxMargin === 5 && p.maxTime === 300) return "clutch";
  if (noSt && p.maxMargin == null && c.rankN === 4 && c.rankMetric === "net")
    return c.rankSide === "top"
      ? "top4"
      : c.rankSide === "bottom"
        ? "bottom4"
        : null;
  return null;
}
export async function readCompareFacts(q: Query, p: Params): Promise<Row[]> {
  const c = p.c,
    a = dbArgs(c),
    d = compareDynamicArgs(p),
    sql = data.sql;
  if (p.empty) return [];
  if (p.kind === "ratings") {
    const variant = presetVariant(p);
    // A savepoint preserves the read-only snapshot if a rolling deployment lacks the preset table.
    if (variant) {
      await q("SAVEPOINT cmp_preset", []);
      try {
        const rows = await q<Row>(
          variant === "overall"
            ? "SELECT * FROM basketball_test.team_ppp_ratings_mv WHERE game_year=$1 ORDER BY rank_net_rtg"
            : "SELECT * FROM basketball_test.team_ratings_preset_cache WHERE game_year=$1 AND preset_variant=$2 ORDER BY rank_net_rtg",
          variant === "overall" ? [c.season] : [c.season, variant],
        );
        await q("RELEASE SAVEPOINT cmp_preset", []);
        return rows;
      } catch {
        await q("ROLLBACK TO SAVEPOINT cmp_preset", []);
        await q("RELEASE SAVEPOINT cmp_preset", []);
      }
    }
    return q<Row>(sql.ratings, d);
  }
  if (p.kind === "four_factors") return q<Row>(sql.four_factors, d);
  if (["lineups", "lineup_ff", "shooting"].includes(p.kind)) {
    const rows = await q<Row>(
      p.kind === "lineup_ff" ? sql.lineup_ff : sql.lineups,
      compareLineupArgs(p),
    );
    return rows.filter((r) => {
      const ids = playerIds(r.player_ids ?? r.sub_lineup_hash);
      return (
        p.on.every((x) => ids.includes(x)) &&
        (!p.any.length || p.any.some((x) => ids.includes(x))) &&
        !p.off.some((x) => ids.includes(x))
      );
    });
  }
  if (p.kind === "players") {
    const args = [
      c.season,
      c.start,
      c.end,
      c.teams.join(",") || null,
      a.typeCsv,
      a.oppCsv,
      a.homeAway,
      a.outcome,
      a.rankSide,
      a.rankN,
      a.rankMetric,
      p.maxMargin,
      null,
      p.maxTime,
      false,
      a.minGn,
      a.maxGn,
      null,
    ];
    // Starters bounds (the team -> players view under Starters vs Bench) are
    // answered only by the action-row reader, as in fetch_player_traditional_filtered().
    const starters = [a.offMin, a.offMax, a.defMin, a.defMax],
      hasStarters = starters.some((x) => x != null);
    const custom =
      hasStarters ||
      (p.maxMargin != null && (p.maxMargin !== 5 || p.maxTime !== 300));
    return addUsagePct(
      normalizeTsRows(
        await q<Row>(
          call(
            custom
              ? "get_player_traditional_custom_clutch"
              : "get_player_traditional_from_games",
            hasStarters
              ? [...playerTypes, ...Array(4).fill("int4")]
              : playerTypes,
          ),
          hasStarters ? [...args, ...starters] : args,
        ),
      ),
    );
  }
  if (p.kind === "player_ff")
    return q<Row>(sql.player_ff, [
      c.season,
      c.start,
      c.end,
      c.teams.join(",") || null,
      a.typeCsv,
      a.oppCsv,
      a.homeAway,
      a.outcome,
      a.rankSide,
      a.rankN,
      a.rankMetric,
      a.minGn,
      a.maxGn,
      null,
      null,
      null,
      a.offMin,
      a.offMax,
      a.defMin,
      a.defMax,
    ]);
  if (p.kind === "onoff")
    return q<Row>(sql.onoff, [
      c.start,
      c.end,
      c.teams.join(",") || null,
      0,
      0,
      -1e9,
      String(c.season),
      a.typeCsv,
      a.oppCsv,
      a.homeAway,
      a.outcome,
      a.rankSide,
      a.rankN,
      a.rankMetric,
      a.minGn,
      a.maxGn,
      null,
      null,
      null,
      a.offMin,
      a.offMax,
      a.defMin,
      a.defMax,
    ]);
  return [];
}
async function read(s: URLSearchParams, p: Params) {
  return withSnapshot(new Deadline(25000, 20000), async (q) => {
    const version = await readVersion(q, "il");
    if (p.kind === "player_defaults")
      return {
        rows: await q<Row>(
          call("get_player_traditional_from_games", playerTypes),
          [p.c.season, ...Array(17).fill(null)],
        ),
        meta: { version, key: s.toString() },
      };
    if (p.kind !== "refs")
      return {
        rows: applyPlayerNames(
          await readCompareFacts(q, p),
          await readPlayerNames(q, "il", p.c.season),
        ),
        meta: { version, key: s.toString() },
      };
    const catalogue = (
      ui.ilTeams as Record<string, { id: number; name: string }[]>
    )[String(p.c.season)];
    const teams: Row[] =
      catalogue?.map((t) => ({ team_id: t.id, team_name: t.name })) ??
      (await q<Row>(
        "SELECT DISTINCT team_id,team_name FROM basketball_test.full_rosters WHERE game_year=$1 ORDER BY team_name",
        [p.c.season],
      ));
    const players = await q<Row>(
      "SELECT team_id,player_id,MIN(btrim(firstname)||' '||btrim(lastname)) AS name FROM basketball_test.full_rosters WHERE game_year=$1 GROUP BY team_id,player_id ORDER BY MIN(btrim(firstname)||' '||btrim(lastname))",
      [p.c.season],
    );
    const named = namedPicker(
      players,
      await readPlayerNames(q, "il", p.c.season),
      "name",
    );
    const rounds = await q<Row>(
      "SELECT DISTINCT gn FROM basketball_test.final_schedule_mv WHERE game_year=$1 AND gn IS NOT NULL ORDER BY gn",
      [p.c.season],
    );
    return {
      rows: [],
      teams,
      players: named,
      rounds: rounds.map((r) => Number(r.gn)),
      meta: { version, key: s.toString() },
    };
  });
}
export type ComparePayload = Awaited<ReturnType<typeof read>>;
const cache = new VersionedCache<ComparePayload>(
  64,
  (value) => value.meta.version,
);
export function compareAnswer(s: URLSearchParams) {
  const p = parseCompare(s),
    key = s.toString();
  return cache.get("il", key, () => read(s, p));
}
