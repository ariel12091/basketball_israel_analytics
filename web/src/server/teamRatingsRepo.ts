import "server-only";
import { withSnapshot, Deadline, type Query } from "./db";
import { parseLineups } from "./lineupParams";
import { ParamError } from "./api";
import { dbArgs, needsFilteredPath, seasonBounds } from "./contextParams";
import { readVersion } from "./gameLogsRepo";
import { VersionedCache } from "./versionedCache";
import { readTabReferences } from "./onoffRepo";
import {
  teamPace,
  traditionalMode,
  teamShotProfile,
  type TeamView,
} from "../rules/teamRatings";
import type { Row, League } from "../rules/gameLogs";
import ui from "../data/shinyUi.json";
export function parseTeamRatings(s: URLSearchParams, league: League) {
  const params = new URLSearchParams(s);
  params.delete("view");
  const p = parseLineups(params, league),
    view = (s.get("view") ?? "summary") as TeamView;
  if (
    !(
      league === "il"
        ? ["summary", "four_factors", "shot_profile", "traditional"]
        : ["summary", "four_factors"]
    ).includes(view)
  )
    throw new ParamError("view", "Invalid view");
  const side = s.get("side") ?? "offense",
    display = s.get("display") ?? "per_game";
  if (!["offense", "defense"].includes(side))
    throw new ParamError("side", "Invalid side");
  if (!["per_game", "per75", "per40"].includes(display))
    throw new ParamError("display", "Invalid display mode");
  if (league === "il") p.c.teams = [];
  return { ...p, view, side, display };
}
type Params = ReturnType<typeof parseTeamRatings>;
const IL_TYPES = [
  "int4",
  "date",
  "date",
  ...Array(5).fill("text"),
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
const call = (fn: string, types: string[]) =>
  `SELECT * FROM ${fn}(${types.map((t, i) => `$${i + 1}::${t}`).join(",")})`;
/** Unchanged Tab 3/9 stored readers and argument order. */
async function readFacts(
  q: Query,
  p: Params,
  kind: "ratings" | "four_factors" | "minutes" | "traditional" | "dashboard",
  end = p.c.end,
): Promise<Row[]> {
  const c = { ...p.c, end },
    a = dbArgs(c),
    il = c.league === "il";
  const head = il
    ? [
        c.season,
        c.start,
        end,
        a.typeCsv,
        a.oppCsv,
        a.homeAway,
        a.outcome,
        a.rankSide,
        a.rankN,
        a.rankMetric,
      ]
    : [
        c.league === "eurocup" ? "U" : "E",
        c.season,
        c.start,
        end,
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
    tail = [
      a.minGn,
      a.maxGn,
      a.lastN,
      ...(il ? [null, null] : []),
      a.offMin,
      a.offMax,
      a.defMin,
      a.defMax,
    ];
  if (il) {
    if (kind === "traditional")
      return q<Row>(
        call("basketball_test.get_team_traditional_dynamic", [
          ...IL_TYPES.slice(0, 17),
          "text",
        ]),
        [...head, ...clutch, a.minGn, a.maxGn, a.lastN, p.side],
      );
    const name = kind === "dashboard" ? "four_factors_dashboard" : kind;
    return q<Row>(call(`basketball_test.get_team_${name}_dynamic`, IL_TYPES), [
      ...head,
      ...clutch,
      ...tail,
    ]);
  }
  const readerKind =
    p.maxMargin == null
      ? "pergame"
      : p.maxMargin === 5 &&
          p.marginStatus === "all" &&
          p.maxTime === 300 &&
          !p.otMargin
        ? "dynamic"
        : "direct";
  const types = [
    ...EL_HEAD,
    ...(readerKind === "pergame" ? [] : ["int4", "text", "int4", "bool"]),
    ...Array(7).fill("int4"),
  ];
  return q<Row>(call(`euroleague.get_team_${kind}_${readerKind}`, types), [
    ...head,
    ...(readerKind === "pergame" ? [] : clutch),
    ...tail,
  ]);
}
/** Tab 3's effective schedule anchor and Tab 9's previous league matchday. */
async function previousEnd(q: Query, p: Params): Promise<string | null> {
  const c = p.c,
    a = dbArgs(c);
  if (c.league !== "il")
    return (
      await q<Row>(
        `SELECT max(game_date) AS d FROM euroleague.team_game_ratings_mv
 WHERE competition=$1::text AND game_year=$2::int4 AND game_date >= $3::date AND game_date <
 (SELECT max(game_date) FROM euroleague.team_game_ratings_mv WHERE competition=$1::text AND game_year=$2::int4 AND game_date <= $4::date)`,
        [c.league === "eurocup" ? "U" : "E", c.season, c.start, c.end],
      )
    )[0]?.d as string | null;
  const anchor =
    (
      await q<Row>(
        `WITH params AS (
 SELECT CASE WHEN $4::text IS NULL OR btrim($4::text)='' THEN NULL::int4[] ELSE string_to_array(regexp_replace($4::text,'\\s+','','g'),',')::int4[] END AS game_types,
 CASE WHEN $5::text IS NULL OR btrim($5::text)='' THEN NULL::int4[] ELSE string_to_array(regexp_replace($5::text,'\\s+','','g'),',')::int4[] END AS opp_ids)
 SELECT MAX(fs.game_date)::date AS max_game_date,MAX(fs.gn)::int AS max_gn FROM basketball_test.final_schedule_mv fs CROSS JOIN params p0
 WHERE fs.game_year=$1::int4 AND ($2::date IS NULL OR fs.game_date >= $2::date) AND ($3::date IS NULL OR fs.game_date <= $3::date)
 AND (p0.game_types IS NULL OR fs.game_type=ANY(p0.game_types)) AND (p0.opp_ids IS NULL OR fs.opp_team_id=ANY(p0.opp_ids))
 AND ($6::text IS NULL OR $6::text='' OR ($6::text='home' AND fs.is_home) OR ($6::text='away' AND NOT fs.is_home))
 AND ($7::text IS NULL OR $7::text='' OR ($7::text='win' AND fs.has_won IS TRUE) OR ($7::text='loss' AND fs.has_won IS FALSE))
 AND ($8::int4 IS NULL OR fs.gn >= $8::int4) AND ($9::int4 IS NULL OR fs.gn <= $9::int4)`,
        [
          c.season,
          c.start,
          c.end,
          a.typeCsv,
          a.oppCsv,
          a.homeAway,
          a.outcome,
          a.minGn,
          a.maxGn,
        ],
      )
    )[0]?.max_game_date ?? c.end;
  return (
    await q<Row>(
      "SELECT MAX(game_date)::date AS d FROM basketball_test.final_schedule_mv WHERE game_year=$1::int4 AND game_date < $2::date",
      [c.season, anchor],
    )
  )[0]?.d as string | null;
}
async function read(q: Query, p: Params) {
  const c = p.c,
    euro = c.league !== "il",
    schema = euro ? "euroleague" : "basketball_test",
    bounds = seasonBounds(c.league, c.season);
  // tr_fallback_needed() also treats the original static UI date range as
  // baseline (older restored links can still carry those literal dates).
  const staticBounds = seasonBounds("il", Number(ui.defaultGameYear));
  const filterContext =
    !euro && c.start === staticBounds.start && c.end === staticBounds.end
      ? { ...c, start: bounds.start, end: bounds.end }
      : c;
  const filtered =
    needsFilteredPath(filterContext) ||
    c.rankN != null ||
    p.maxMargin != null ||
    (euro && c.teams.length > 0);
  const showDelta =
    c.start === bounds.start &&
    c.end === bounds.end &&
    c.minGn == null &&
    c.maxGn == null &&
    c.lastN == null;
  const dynamicClutch =
    p.maxMargin === 5 &&
    p.marginStatus === "all" &&
    p.maxTime === 300 &&
    !p.otMargin;
  const dashboard =
    filtered && dynamicClutch && (euro || p.view === "four_factors");
  const fact =
    p.view === "traditional"
      ? "traditional"
      : p.view === "four_factors"
        ? "four_factors"
        : "ratings";
  const version = await readVersion(q, c.league);
  const current = dashboard
    ? await readFacts(q, p, "dashboard")
    : p.view === "traditional" || filtered
      ? await readFacts(q, p, fact)
      : await q<Row>(
          `SELECT * FROM ${schema}.team_${fact === "ratings" ? "ppp_ratings" : "four_factors"}_mv WHERE ${euro ? "competition=$1::text AND game_year=$2::int4" : "game_year=$1::int4"}${fact === "ratings" ? " ORDER BY rank_net_rtg" : ""}`,
          euro ? [c.league === "eurocup" ? "U" : "E", c.season] : [c.season],
        );
  const minutes =
    p.view === "traditional"
      ? []
      : dashboard
        ? current
        : await readFacts(q, p, "minutes");
  let rows =
    p.view === "traditional"
      ? traditionalMode(current, p.display)
      : teamPace(current, minutes, euro);
  if (p.view === "shot_profile") {
    const ff = filtered
      ? await readFacts(q, p, dynamicClutch ? "dashboard" : "four_factors")
      : await q<Row>(
          "SELECT * FROM basketball_test.team_four_factors_mv WHERE game_year=$1::int4",
          [c.season],
        );
    rows = teamShotProfile(rows, ff);
  }
  const prevEnd =
    showDelta && p.view !== "shot_profile" ? await previousEnd(q, p) : null;
  let previous: Row[] = prevEnd
    ? await readFacts(q, p, dashboard ? "dashboard" : fact, prevEnd)
    : [];
  if (p.view === "traditional") previous = traditionalMode(previous, p.display);
  return {
    rows,
    previous,
    meta: {
      ...(await readTabReferences(q, c, version)),
      version,
      view: p.view,
      side: p.side,
      display: p.display,
      showDelta,
      previousEnd: prevEnd,
      filtered,
    },
  };
}
const cache = new VersionedCache<Awaited<ReturnType<typeof read>>>(
  16,
  (value) => value.meta.version,
);
export function teamRatingsAnswer(league: League, s: URLSearchParams) {
  const p = parseTeamRatings(s, league),
    key = JSON.stringify(p);
  return cache.get(league, key, () =>
    withSnapshot(new Deadline(25000, 20000), (q) => read(q, p)),
  );
}
