// Tab 7 collect_side_params / apply_compare_preset / PRESET_MIRROR_BLOCKS.
import type { Query, Patch } from "./tabQuery";
import { seasonBounds } from "./tabQuery";
import data from "../data/compareUi.json";
import type { Row } from "../rules/gameLogs";
export type CompareSelection = { season: string; a: string; b: string };
export function withCompareSelection(
  q: Query,
  selection?: CompareSelection,
): Query {
  return selection?.season === q.season &&
    q.view === "Players" &&
    q.auto_players === "true" &&
    !q.player_a &&
    !q.player_b
    ? { ...q, player_a: selection.a, player_b: selection.b }
    : q;
}
export function comparePlayerTeams(
  q: Query,
  side: CompareSide,
  players: Row[],
) {
  const key = q.player_compare === "self" ? "a" : side,
    explicit = q[`player_team_${key}`],
    id = q[`player_${key}`];
  const ids = [
    ...new Set(
      players
        .filter((r) => String(r.player_id) === id)
        .map((r) => String(r.team_id)),
    ),
  ];
  return explicit && ids.includes(explicit) ? explicit : ids.join(",");
}
export type CompareMode = "Teams" | "Lineups" | "Players";
export type CompareSide = "a" | "b";
export const SIDE_FIELDS = [
  "own_starters",
  "opp_starters",
  "teams",
  "home_away",
  "outcome",
  "clutch",
  "clutch_margin",
  "clutch_minutes",
  "opponents",
  "game_type",
  "opp_rank_side",
  "opp_rank_n",
  "opp_rank_metric",
];
export const compareDefaults: Query = {
  view: "Teams",
  preset: "",
  team_id: "",
  open_detail: "false",
  min_poss: "10",
  auto_min: "true",
  split_date: "",
  split_gn: "",
  cmp_view: "league",
  entity: "",
  size: "5",
  lu_team: "",
  lu_on: "",
  lu_any: "",
  lu_off: "",
  player_compare: "other",
  player_a: "",
  player_b: "",
  player_team_a: "",
  player_team_b: "",
  list_a: "",
  list_b: "",
  player_view: "overall",
  rate: "Per Game",
  team_rate: "Per Game",
  team_metric: "pts",
  auto_players: "true",
};
Object.assign(compareDefaults, {
  metric: "net_rtg",
  drill_player: "",
  preset_applied: "false",
});
for (const side of ["a", "b"]) {
  for (const key of SIDE_FIELDS)
    compareDefaults[`${side}_${key}`] =
      key === "clutch"
        ? "false"
        : key === "clutch_margin" || key === "clutch_minutes"
          ? "5"
          : "";
  for (const key of ["start", "end", "min_gn", "max_gn"])
    compareDefaults[`${side}_${key}`] = "";
}
export function resetCompare(): Patch {
  return {
    ...compareDefaults,
    start: null,
    end: null,
    min_gn: null,
    max_gn: null,
    stats: null,
  };
}
export function applyPreset(q: Query, preset: string): Patch {
  const out: Patch = {
    preset,
    auto_min: "true",
    entity: null,
    preset_applied: "true",
  };
  for (const s of ["a", "b"])
    for (const k of SIDE_FIELDS)
      if (!["clutch_margin", "clutch_minutes"].includes(k))
        out[`${s}_${k}`] = k === "clutch" ? "false" : null;
  const pair = (key: string, a: string, b: string) =>
    Object.assign(out, { [`a_${key}`]: a, [`b_${key}`]: b });
  if (preset === "starters_bench") pair("own_starters", "gte:3", "lte:2");
  if (preset === "opp_starters_bench") pair("opp_starters", "gte:3", "lte:2");
  if (preset === "home_away") pair("home_away", "home", "away");
  if (preset === "win_loss") pair("outcome", "win", "loss");
  if (preset === "clutch")
    Object.assign(out, {
      a_clutch: "true",
      a_clutch_margin: "5",
      a_clutch_minutes: "5",
    });
  if (preset === "top_bottom_rank") {
    pair("opp_rank_side", "top", "bottom");
    pair("opp_rank_n", "4", "4");
    pair("opp_rank_metric", "net", "net");
  }
  if (preset === "date_split")
    Object.assign(out, {
      start: null,
      end: null,
      split_date: q.split_date || seasonBounds("il", q.season).end,
    });
  if (preset === "gn_split") Object.assign(out, { min_gn: null, max_gn: null });
  return out;
}
export function editCompareSide(
  q: Query,
  s: CompareSide,
  key: string,
  value: string,
): Patch {
  const out: Patch = { [`${s}_${key}`]: value, auto_min: "true" };
  const blockers = data.PRESET_MIRROR_BLOCKS as Record<
    string,
    string | string[]
  >;
  const blocked = [blockers[q.preset] ?? []].flat();
  const rName =
    key === "own_starters"
      ? "starters_mode"
      : key === "opp_starters"
        ? "opp_starters_mode"
        : key;
  if (q.view !== "Players" && q.preset && !blocked.includes(rName))
    out[`${s === "a" ? "b" : "a"}_${key}`] = value;
  if (
    q.view === "Players" &&
    q.auto_players === "true" &&
    ["game_type", "opponents", "home_away", "outcome", "clutch"].includes(key)
  )
    Object.assign(out, {
      player_a: null,
      player_b: null,
      auto_players: "false",
    });
  return out;
}
export function compareSideParams(
  q: Query,
  side: CompareSide,
  kind: string,
): URLSearchParams {
  const b = seasonBounds("il", q.season),
    s = new URLSearchParams({ season: q.season, kind });
  const self = q.view === "Players" && q.player_compare === "self";
  let start = (self ? q[`${side}_start`] : q.start) || b.start,
    end = (self ? q[`${side}_end`] : q.end) || b.end;
  let min = (self ? q[`${side}_min_gn`] : q.min_gn) || "",
    max = (self ? q[`${side}_max_gn`] : q.max_gn) || "";
  if (q.view !== "Players" && q.preset === "date_split") {
    start = b.start;
    end = b.end;
    if (q.split_date) {
      if (side === "a") end = q.split_date;
      else {
        const d = new Date(q.split_date + "T00:00:00Z");
        d.setUTCDate(d.getUTCDate() + 1);
        start = d.toISOString().slice(0, 10);
      }
    }
  }
  if (q.view !== "Players" && q.preset === "gn_split") {
    min = "";
    max = "";
    if (q.split_gn) {
      if (side === "a") max = q.split_gn;
      else min = String(Number(q.split_gn) + 1);
    }
  }
  s.set("start", start);
  s.set("end", end);
  if (min) s.set("min_gn", min);
  if (max) s.set("max_gn", max);
  if (start > end || (min && max && Number(min) > Number(max))) {
    s.set("start", b.start);
    s.set("end", b.end);
    s.delete("min_gn");
    s.delete("max_gn");
    s.set("empty", "true");
  }
  for (const k of SIDE_FIELDS) {
    const v = q[`${side}_${k}`];
    if (
      v &&
      (!["own_starters", "opp_starters"].includes(k) ||
        /^(gte|lte):[0-5]$/.test(v)) &&
      (q.view !== "Players" ||
        !["own_starters", "opp_starters", "teams"].includes(k))
    )
      s.set(k, v);
  }
  if (q.view === "Lineups") {
    s.set("size", q.size);
    const sideTeams = (q[`${side}_teams`] || "").split(",").filter(Boolean);
    if (q.lu_team && sideTeams.length && !sideTeams.includes(q.lu_team))
      s.set("empty", "true");
    if (q.lu_team) {
      s.set("teams", q.lu_team);
      for (const k of ["on", "any", "off"])
        if (q[`lu_${k}`]) s.set(`players_${k}`, q[`lu_${k}`]);
    }
  }
  return s;
}
export const comparePath = (s: URLSearchParams) => `/api/v1/il/compare?${s}`;
/** URL form of Shiny's pending_compare_preset payload from Home storylines. */
export function comparePresetHref(
  season: string,
  preset: string,
  teamId: string,
) {
  return `/il/compare?${new URLSearchParams({ season, preset, team_id: teamId, open_detail: "true" })}`;
}
export function resolveCompareQuery(q: Query): Query {
  if (q.preset && q.preset_applied !== "true") {
    const team = /^[1-9]\d*$/.test(q.team_id) ? q.team_id : "";
    return {
      ...q,
      ...Object.fromEntries(
        Object.entries(applyPreset(q, q.preset)).map(([k, v]) => [
          k,
          v ?? compareDefaults[k] ?? "",
        ]),
      ),
      entity: q.entity,
      ...(team ? { a_teams: team, b_teams: team } : {}),
      ...(team && q.open_detail === "true"
        ? { view: "Teams", entity: team, cmp_view: "detail" }
        : {}),
    };
  }
  return q;
}
export function shortSideLabel(q: Query, s: CompareSide) {
  const pair: Record<string, [string, string]> = {
    starters_bench: ["Starters", "Bench"],
    opp_starters_bench: ["vs Starters", "vs Bench"],
    clutch: ["Clutch", "Non-Clutch"],
    home_away: ["Home", "Away"],
    win_loss: ["Win", "Loss"],
  };
  if (q.view === "Players") return s.toUpperCase();
  if (pair[q.preset]) return pair[q.preset][s === "a" ? 0 : 1];
  if (q.preset === "top_bottom_rank" && q[`${s}_opp_rank_n`])
    return `vs ${s === "a" ? "Top" : "Bottom"} ${q[`${s}_opp_rank_n`]}`;
  if (q.preset === "gn_split" && q.split_gn)
    return s === "a" ? `GN 1-${q.split_gn}` : `GN ${Number(q.split_gn) + 1}+`;
  if (q.preset === "date_split" && q.split_date) {
    const d = new Date(q.split_date + "T00:00:00Z");
    return `${s === "a" ? "Before" : "From"} ${d.toLocaleDateString("en-US", { month: "short", day: "2-digit", timeZone: "UTC" })}`;
  }
  return s.toUpperCase();
}

// server_tab7_compare.R side_label_full(): context descriptions, distinct
// from the short table headings (notably Clutch's overall side B).
export function fullSideLabel(
  q: Query,
  side: CompareSide,
  teams: { value: string; label: string }[],
) {
  const get = (key: string) => q[`${side}_${key}`] || "",
    parts: string[] = [],
    title = (value: string) => value[0]?.toUpperCase() + value.slice(1),
    names = new Map(teams.map((t) => [t.value, t.label])),
    ids = (key: string) => get(key).split(",").filter(Boolean);
  for (const [key, label] of [
    ["own_starters", "Own starters"],
    ["opp_starters", "Opp starters"],
  ]) {
    const match = /^(gte|lte):([0-5])$/.exec(get(key));
    if (match)
      parts.push(`${label} ${match[1] === "gte" ? ">=" : "<="}${match[2]}`);
  }
  for (const key of ["home_away", "outcome"])
    if (get(key)) parts.push(title(get(key)));
  if (get("clutch") === "true") parts.push("Clutch");
  if (get("opp_rank_side") && get("opp_rank_n")) {
    const metric = (
      { off: "Off", def: "Def", net: "Net" } as Record<string, string>
    )[get("opp_rank_metric")];
    parts.push(
      `vs ${title(get("opp_rank_side"))} ${get("opp_rank_n")}${metric ? ` (${metric})` : ""}`,
    );
  }
  for (const key of ["opponents", "teams"]) {
    const values = ids(key),
      labels = values.map((id) => names.get(id) ?? id);
    if (values.length)
      parts.push(
        key === "opponents"
          ? `vs ${values.length <= 2 ? labels.join(", ") : `${values.length} opps`}`
          : values.length <= 2
            ? labels.join(", ")
            : `${values.length} teams`,
      );
  }
  const gameTypes = ids("game_type")
    .map((id) => (data.gt_labels as Record<string, string>)[id])
    .filter(Boolean);
  if (gameTypes.length) parts.push(gameTypes.join("+"));
  if (q.view !== "Players") {
    if (q.preset === "date_split" && q.split_date)
      parts.push(`${side === "a" ? "Before" : "After"} ${q.split_date}`);
    if (q.preset === "gn_split" && q.split_gn)
      parts.push(`${side === "a" ? "Before" : "After"} GN ${q.split_gn}`);
  }
  return parts.join(" | ") || `Side ${side.toUpperCase()}`;
}
