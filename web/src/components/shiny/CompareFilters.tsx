"use client";
import type { Query, Patch } from "@/lib/tabQuery";
import { seasonBounds } from "@/lib/tabQuery";
import {
  editCompareSide,
  fullSideLabel,
  oppRankMissing,
  shortSideLabel,
  LAST_N_DEFAULT,
  LAST_N_PRESET,
  type CompareSide,
} from "@/lib/compareQuery";
import type { Row } from "@/rules/gameLogs";
import Selectize, { type Choice } from "./Selectize";
import DateRangeInput from "./DateRangeInput";
import { StarterSelects } from "./TabKit";
import layout from "@/data/layoutUi.json";
import type { ReactNode } from "react";
import catalogues from "@/data/catalogues.json";
import { choiceValues } from "@/lib/choices";
/** As every tab's Last N control: up to the season's game numbers. Never below
 * the preset's default, which must show even early in a season. */
export const lastNChoices = (rounds: number[]): Choice[] =>
  Array.from(
    { length: Math.max(Number(LAST_N_DEFAULT), ...rounds) },
    (_, i) => ({ value: String(i + 1), label: String(i + 1) }),
  );
export function CompareSelect({
  id,
  label,
  value,
  choices,
  onChange,
  multiple = false,
  placeholder = "All",
}: {
  id: string;
  label: string;
  value: string;
  choices: Choice[];
  onChange: (value: string) => void;
  multiple?: boolean;
  placeholder?: string;
}) {
  return (
    <Selectize
      id={id}
      label={label}
      value={choiceValues(value, choices)}
      choices={choices}
      multiple={multiple}
      placeholder={placeholder}
      emptyLabel={multiple ? undefined : placeholder}
      onChange={(xs) => onChange(xs.join(","))}
    />
  );
}
export function CompareGn({
  q,
  set,
  rounds,
  edge,
  prefix = "",
}: {
  q: Query;
  set: (p: Patch) => void;
  rounds: number[];
  edge: "min" | "max";
  prefix?: string;
}) {
  const key = `${prefix}${edge}_gn`;
  return (
    <CompareSelect
      id={`cmp_${prefix}gn_${edge}`}
      label={edge === "min" ? GN_LABELS.min : GN_LABELS.max}
      placeholder="Any"
      value={q[key]}
      choices={rounds.map((n) => ({ value: String(n), label: String(n) }))}
      onChange={(v) => set({ [key]: v, auto_min: "true" })}
    />
  );
}
const GN_LABELS = {
  min: "From Game Number (GN)",
  max: "To Game Number (GN)",
};
export function CompareDates({
  q,
  set,
  rounds,
  prefix = "",
  showDates = true,
  showGN = true,
}: {
  q: Query;
  set: (p: Patch) => void;
  rounds: number[];
  prefix?: string;
  showDates?: boolean;
  showGN?: boolean;
}) {
  const bounds = seasonBounds("il", q.season),
    key = (s: string) => `${prefix}${s}`;
  return (
    <>
      {showDates && (
        <DateRangeInput
          id={`cmp_${prefix}dates`}
          label="Date range"
          start={q[key("start")] || bounds.start}
          end={q[key("end")] || bounds.end}
          min={bounds.start}
          max={bounds.end}
          onChange={(a, b) =>
            set({ [key("start")]: a, [key("end")]: b, auto_min: "true" })
          }
        />
      )}
      {showGN && (
        <div className="row">
          <div className="col-6">
            <CompareGn
              q={q}
              set={set}
              rounds={rounds}
              edge="min"
              prefix={prefix}
            />
          </div>
          <div className="col-6">
            <CompareGn
              q={q}
              set={set}
              rounds={rounds}
              edge="max"
              prefix={prefix}
            />
          </div>
        </div>
      )}
    </>
  );
}
/** What a side is, in words, so nobody has to read its fields to find out. */
function sideHeading(
  q: Query,
  side: CompareSide,
  teams: Choice[],
  players: Row[],
) {
  const letter = side.toUpperCase(),
    short = shortSideLabel(q, side),
    full = fullSideLabel(q, side, teams),
    named = short !== letter,
    // "vs Top 4" and "vs Top 4 (Net)" say one thing: keep the longer.
    extended = named && full.startsWith(short),
    detail = extended || full === `Side ${letter}` ? "" : full;
  if (q.view === "Players") {
    const id = q[`player_${q.player_compare === "self" ? "a" : side}`],
      name = players.find((r) => String(r.player_id) === id)?.name;
    return { heading: name ? String(name) : "", detail };
  }
  return { heading: extended ? full : named ? short : "", detail };
}
const SIDES = ["a", "b"] as const;
const HINTS: Record<string, string> = layout.compareFilters;
const TEXT = layout.compareGrid;
type Field = {
  key: string;
  label: string;
  /** The side fields whose values say whether A and B differ on this row. */
  diff: string[];
  control: (side: CompareSide) => ReactNode;
};
/** One row per filter, with A and B next to each other. */
export default function CompareSidesGrid({
  q,
  set,
  teams,
  players,
  rounds,
  onToggle,
}: {
  q: Query;
  set: (p: Patch) => void;
  teams: Choice[];
  players: Row[];
  rounds: number[];
  onToggle: () => void;
}) {
  const update = (side: CompareSide, k: string, v: string) =>
      set(editCompareSide(q, side, k, v)),
    playerMode = q.view === "Players",
    self = q.player_compare === "self";
  // A filter the two sides set differently is what this comparison is about.
  const differs = (keys: string[]) =>
    keys.some((k) => {
      const value = (s: CompareSide) =>
        (q[`${s}_${k}`] ?? "").split(",").sort().join(",");
      return value("a") !== value("b");
    });
  const select = (
    key: string,
    label: string,
    choices: Choice[],
    multiple = false,
    placeholder = "All",
  ): Field => ({
    key,
    label,
    diff: [key],
    control: (side) => (
      <CompareSelect
        id={`cmp_${side}_${key}`}
        label={label}
        value={q[`${side}_${key}`]}
        choices={choices}
        onChange={(v) => update(side, key, v)}
        multiple={multiple}
        placeholder={placeholder}
      />
    ),
  });
  const starters = (
    key: "own_starters" | "opp_starters",
    label: string,
  ): Field => ({
    key,
    label,
    diff: [key],
    control: (side) => (
      <StarterSelects
        p={`cmp_${side}`}
        query={{ ...q, [key]: q[`${side}_${key}`] }}
        set={(patch) => {
          for (const [k, v] of Object.entries(patch)) update(side, k, v ?? "");
        }}
        starterKey={key}
        wrap={(mode, value) => (
          <div className="cmp-grid-pair">
            {mode}
            {value}
          </div>
        )}
      />
    ),
  });
  const playerTeams = (side: CompareSide) => [
    ...new Set(
      players
        .filter((r) => String(r.player_id) === q[`player_${side}`])
        .map((r) => String(r.team_id)),
    ),
  ];
  // "Against himself" has one player, chosen on side A.
  const hasPicker = (side: CompareSide) => !self || side === "a";
  const selfSet = (patch: Patch) =>
    set({
      ...patch,
      ...(q.auto_players === "true"
        ? { player_a: null, player_b: null, auto_players: "false" }
        : {}),
    });
  const selfDates: Field[] =
    playerMode && self
      ? [
          {
            key: "dates",
            label: "Date range",
            diff: ["start", "end"],
            control: (side) => (
              <CompareDates
                q={q}
                set={selfSet}
                rounds={rounds}
                prefix={`${side}_`}
                showGN={false}
              />
            ),
          },
          ...(["min", "max"] as const).map((edge): Field => ({
            key: `${edge}_gn`,
            label: GN_LABELS[edge],
            diff: [`${edge}_gn`],
            control: (side) => (
              <CompareGn
                q={q}
                set={selfSet}
                rounds={rounds}
                edge={edge}
                prefix={`${side}_`}
              />
            ),
          })),
        ]
      : [];
  const playerFields: Field[] = playerMode
    ? [
        {
          key: "list",
          label: "Player List Team Filter",
          diff: [],
          control: (side) =>
            hasPicker(side) && (
              <CompareSelect
                id={`cmp_player_${side}_list_team_filter`}
                label="Player List Team Filter"
                value={q[`list_${side}`]}
                choices={teams}
                multiple
                onChange={(v) =>
                  set({
                    [`list_${side}`]: v,
                    ...(v &&
                    !players.some(
                      (r) =>
                        String(r.player_id) === q[`player_${side}`] &&
                        v.split(",").includes(String(r.team_id)),
                    )
                      ? { [`player_${side}`]: null }
                      : {}),
                  })
                }
              />
            ),
        },
        {
          key: "player",
          label: "Player",
          diff: [],
          control: (side) => {
            if (!hasPicker(side)) return null;
            const listTeams = q[`list_${side}`].split(",").filter(Boolean),
              names = new Map(
                players
                  .filter(
                    (r) =>
                      !listTeams.length ||
                      listTeams.includes(String(r.team_id)),
                  )
                  .map((r) => [String(r.player_id), String(r.name)]),
              );
            return (
              <CompareSelect
                id={`cmp_player_${side}`}
                label="Player"
                placeholder="Search player..."
                value={q[`player_${side}`]}
                choices={[...names].map(([value, label]) => ({ value, label }))}
                onChange={(v) =>
                  set({
                    [`player_${side}`]: v,
                    [`player_team_${side}`]: null,
                    auto_players: "false",
                  })
                }
              />
            );
          },
        },
        ...(SIDES.some((s) => hasPicker(s) && playerTeams(s).length > 1)
          ? [
              {
                key: "player_team",
                label: "Team (optional)",
                diff: [],
                control: (side: CompareSide) =>
                  hasPicker(side) &&
                  playerTeams(side).length > 1 && (
                    <CompareSelect
                      id={`cmp_player_${side}_team`}
                      label="Team (optional)"
                      value={q[`player_team_${side}`]}
                      choices={teams.filter((t) =>
                        playerTeams(side).includes(t.value),
                      )}
                      onChange={(v) => set({ [`player_team_${side}`]: v })}
                    />
                  ),
              },
            ]
          : []),
      ]
    : [];
  const clutchOn = (side: CompareSide) => q[`${side}_clutch`] === "true";
  const slider = (
    key: string,
    label: string,
    min: number,
    max: number,
  ): Field => ({
    key,
    label,
    diff: [key],
    control: (side) =>
      clutchOn(side) && (
        <div className="cmp-grid-range">
          <input
            id={`cmp_${side}_${key}`}
            type="range"
            aria-label={`${label}, side ${side.toUpperCase()}`}
            min={min}
            max={max}
            step={1}
            value={q[`${side}_${key}`]}
            onChange={(e) => update(side, key, e.target.value)}
          />
          <output>{q[`${side}_${key}`]}</output>
        </div>
      ),
  });
  const fields: Field[] = [
    ...selfDates,
    ...playerFields,
    ...(playerMode
      ? []
      : [
          starters("own_starters", "Own lineup starters"),
          starters("opp_starters", "Opponent lineup starters"),
          select("teams", "Teams", teams, true),
        ]),
    select("home_away", "Home/Away", [
      { value: "home", label: "Home" },
      { value: "away", label: "Away" },
    ]),
    select("outcome", "Win/Loss", [
      { value: "win", label: "Win" },
      { value: "loss", label: "Loss" },
    ]),
    {
      key: "clutch",
      label: "Clutch",
      diff: ["clutch"],
      control: (side) => (
        <input
          type="checkbox"
          className="cmp-grid-check"
          id={`cmp_${side}_clutch`}
          aria-label={`Clutch, side ${side.toUpperCase()}`}
          checked={clutchOn(side)}
          onChange={(e) => update(side, "clutch", String(e.target.checked))}
        />
      ),
    },
    ...(SIDES.some(clutchOn)
      ? [
          slider("clutch_margin", "Max point margin", 0, 10),
          slider("clutch_minutes", "Max minutes remaining", 1, 5),
        ]
      : []),
    select("opponents", "Opponents", teams, true),
    select(
      "game_type",
      "Game type",
      catalogues.gameTypeChoices.filter((c) => c.value !== ""),
      true,
    ),
    ...(!playerMode && q.preset !== LAST_N_PRESET
      ? [
          select(
            "last_n",
            "Last N Team Games",
            lastNChoices(rounds),
            false,
            "Any",
          ),
        ]
      : []),
  ];
  // One filter in three fields, so they sit in one box under one name.
  const rankFields: Field[] = [
    select(
      "opp_rank_side",
      "Top / Bottom",
      [
        { value: "top", label: "Top" },
        { value: "bottom", label: "Bottom" },
      ],
      false,
      "Off",
    ),
    select(
      "opp_rank_n",
      "Rank N",
      Array.from({ length: 12 }, (_, i) => ({
        value: String(i + 1),
        label: String(i + 1),
      })),
      false,
      "--",
    ),
    select(
      "opp_rank_metric",
      "Metric",
      [
        { value: "off", label: "Offense" },
        { value: "def", label: "Defense" },
        { value: "net", label: "Net rating" },
      ],
      false,
      "--",
    ),
  ];
  const row = (f: Field) => (
    <div
      key={f.key}
      role="group"
      aria-label={f.label}
      className={`cmp-grid-row${differs(f.diff) ? " is-diff" : ""}`}
    >
      <div className="cmp-grid-label">
        <span className="cmp-grid-name">{f.label}</span>
        <small className="cmp-grid-hint">{HINTS[f.key]}</small>
      </div>
      {SIDES.map((side) => (
        <div key={side} className={`cmp-grid-cell is-${side}`}>
          {f.control(side)}
        </div>
      ))}
    </div>
  );
  return (
    <div className="cmp-grid">
      {SIDES.map((side) => {
        const { heading, detail } = sideHeading(q, side, teams, players);
        return (
          <div key={side} className={`cmp-grid-head is-${side}`}>
            <div className="cmp-grid-title">
              <span className="cmp-grid-letter">{side.toUpperCase()}</span>
              {heading || `Side ${side.toUpperCase()}`}
            </div>
            <small className="cmp-grid-detail">
              {detail || (heading ? "" : "No filters on this side")}
            </small>
          </div>
        );
      })}
      {/* The headers say what is compared, so the rows can fold away. Which
          label shows is width-dependent, so web.css picks it (see cmp-layout). */}
      <button type="button" className="cmp-grid-toggle" onClick={onToggle}>
        <span className="cmp-toggle-show">{TEXT.showFilters}</span>
        <span className="cmp-toggle-hide">{TEXT.hideFilters}</span>
      </button>
      {fields.map(row)}
      <div role="group" aria-label={TEXT.rankName} className="cmp-grid-group">
        <div className="cmp-grid-group-head">
          <span className="cmp-grid-name">{TEXT.rankName}</span>
          <small className="cmp-grid-hint">{TEXT.rankHint}</small>
        </div>
        {rankFields.map(row)}
        {/* The readers ignore a half-set rank, so say so instead of showing
            numbers that look filtered. */}
        {SIDES.some((s) => oppRankMissing(q, s).length > 0) && (
          <>
            <div className="cmp-grid-corner" aria-hidden="true" />
            {SIDES.map((s) => {
              const missing = oppRankMissing(q, s);
              return (
                <div key={s} className="cmp-grid-warning" role="status">
                  {missing.length > 0 &&
                    `${TEXT.rankNotApplied} ${missing.join(" and ")}`}
                </div>
              );
            })}
          </>
        )}
      </div>
    </div>
  );
}
