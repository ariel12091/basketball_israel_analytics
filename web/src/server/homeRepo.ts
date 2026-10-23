import "server-only";
import { withSnapshot, Deadline, type Query } from "./db";
import { readVersion } from "./gameLogsRepo";
import { ParamError } from "./api";
import { seasonBounds } from "./contextParams";
import { homeTeams } from "../lib/homeQuery";
import type { Row } from "../rules/gameLogs";
import type { HomeFrames } from "../rules/home";
import ui from "../data/shinyUi.json";
import home from "../data/homeUi.json";
export function parseHome(s: URLSearchParams) {
  const season = s.get("season") ?? ui.defaultGameYear,
    team = s.get("team_id") ?? "";
  if (!ui.ilSeasons.includes(season))
    throw new ParamError("season", "Invalid season");
  if (
    !/^\d+$/.test(team) ||
    !homeTeams(season).some((t) => String(t.id) === team)
  )
    throw new ParamError("team_id", "Invalid team");
  return { season: Number(season), team: Number(team) };
}
export function emptyHomeFrames(): HomeFrames {
  return {
    ratings: [],
    four_factors: [],
    onoff: [],
    traditional: [],
    lineups: [],
    storylines: [],
    last_games: [],
  };
}
export function parseHomeDashboard(
  row: Record<string, unknown> | undefined,
): HomeFrames | null {
  if (!row) return null;
  const frames = emptyHomeFrames();
  try {
    for (const [section, required] of Object.entries(home.contracts)) {
      const value = row[`${section}_json`];
      const rows = typeof value === "string" ? JSON.parse(value) : value;
      if (
        !Array.isArray(rows) ||
        !rows.every((r) => r && typeof r === "object" && !Array.isArray(r))
      )
        return null;
      if (
        ["storylines", "ratings", "four_factors"].includes(section) &&
        !rows.length
      )
        return null;
      if (rows.length && !rows.every((r) => required.every((col) => col in r)))
        return null;
      frames[section as keyof HomeFrames] = rows;
    }
  } catch {
    return null;
  }
  return frames;
}
export async function readHome(q: Query, p: ReturnType<typeof parseHome>) {
  const version = await readVersion(q, "il"),
    errors: string[] = [];
  let serial = 0;
  async function safe<T = Row>(
    section: string,
    sql: string,
    params: unknown[],
  ): Promise<T[] | null> {
    const save = `home_${serial++}`;
    await q(`SAVEPOINT ${save}`, []);
    try {
      const rows = await q<T>(sql, params);
      await q(`RELEASE SAVEPOINT ${save}`, []);
      return rows;
    } catch (e) {
      // A failed SQL statement aborts its transaction until rolled back.
      await q(`ROLLBACK TO SAVEPOINT ${save}`, []);
      await q(`RELEASE SAVEPOINT ${save}`, []);
      if ((e as { code?: string }).code === "57014") throw e;
      errors.push(section);
      return null;
    }
  }
  const combined = await safe<Record<string, unknown>>(
    "combined",
    home.sql.dashboard,
    [p.season, p.team],
  );
  let frames = combined?.length === 1 ? parseHomeDashboard(combined[0]) : null;
  const source = frames ? "combined" : "fallback";
  if (!frames) {
    frames = emptyHomeFrames();
    const sql = home.sql,
      bounds = seasonBounds("il", p.season);
    frames.ratings =
      (await safe("ratings", sql.hub_fetch_team_ratings, [p.season])) ?? [];
    frames.four_factors =
      (await safe("four_factors", sql.hub_fetch_team_ff, [p.season])) ?? [];
    frames.onoff = (await safe("onoff", sql.hub_onoff_df, [p.season])) ?? [];
    frames.traditional =
      (await safe("traditional", sql.hub_ts_df, [p.season])) ?? [];
    frames.lineups =
      (await safe("lineups", sql.hub_lineups_df, [
        5,
        String(p.team),
        null,
        null,
        false,
        bounds.start,
        bounds.end,
        100,
        p.season,
        ...Array(10).fill(null),
        false,
        ...Array(9).fill(null),
      ])) ?? [];
    const persisted = await safe(
      "storyline_presets",
      sql.hub_fetch_team_ratings_presets,
      [p.season],
    );
    frames.storylines =
      persisted ??
      (await safe("storylines", sql.storylines_dynamic, [p.season])) ??
      [];
  }
  frames.last_games =
    (await safe("last_games", home.sql.hub_fetch_last_games, [p.season])) ?? [];
  // A recovered reader is not a missing section; expose only unavailable data.
  const missing = errors.filter(
    (s) => s !== "combined" && s !== "storyline_presets",
  );
  return {
    frames,
    meta: {
      version,
      season: String(p.season),
      teamId: String(p.team),
      source,
      errors: missing,
    },
  };
}
export type HomePayload = Awaited<ReturnType<typeof readHome>>;
const cache = new Map<
  string,
  { expires: number; promise: Promise<HomePayload> }
>();
export function homeAnswer(s: URLSearchParams) {
  const p = parseHome(s),
    key = `${p.season}:${p.team}`,
    now = Date.now(),
    hit = cache.get(key);
  if (hit && hit.expires > now) return hit.promise;
  for (const [k, v] of cache) if (v.expires <= now) cache.delete(k);
  if (cache.size >= 64) cache.delete(cache.keys().next().value!);
  const promise = withSnapshot(new Deadline(25000, 20000), (q) =>
    readHome(q, p),
  );
  cache.set(key, { expires: now + 60000, promise });
  void promise.then(
    (data) => {
      if (data.meta.errors.length && cache.get(key)?.promise === promise)
        cache.delete(key);
    },
    () => {
      if (cache.get(key)?.promise === promise) cache.delete(key);
    },
  );
  return promise;
}
