"use client";
import type { Query, Patch } from "@/lib/tabQuery";
import { seasonBounds } from "@/lib/tabQuery";
import { editCompareSide, type CompareSide } from "@/lib/compareQuery";
import type { Row } from "@/rules/gameLogs";
import Selectize, { type Choice } from "./Selectize";
import DateRangeInput from "./DateRangeInput";
import { StarterRows } from "./TabKit";
import catalogues from "@/data/catalogues.json";
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
      value={value.split(",").filter(Boolean)}
      choices={choices}
      multiple={multiple}
      placeholder={placeholder}
      emptyLabel={multiple ? undefined : placeholder}
      onChange={(xs) => onChange(xs.join(","))}
    />
  );
}
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
            <CompareSelect
              id={`cmp_${prefix}gn_min`}
              label="From Game Number (GN)"
              placeholder="Any"
              value={q[key("min_gn")]}
              choices={rounds.map((n) => ({
                value: String(n),
                label: String(n),
              }))}
              onChange={(v) => set({ [key("min_gn")]: v, auto_min: "true" })}
            />
          </div>
          <div className="col-6">
            <CompareSelect
              id={`cmp_${prefix}gn_max`}
              label="To Game Number (GN)"
              placeholder="Any"
              value={q[key("max_gn")]}
              choices={rounds.map((n) => ({
                value: String(n),
                label: String(n),
              }))}
              onChange={(v) => set({ [key("max_gn")]: v, auto_min: "true" })}
            />
          </div>
        </div>
      )}
    </>
  );
}
export default function CompareSideFilters({
  q,
  set,
  side,
  teams,
  players,
  rounds,
}: {
  q: Query;
  set: (p: Patch) => void;
  side: CompareSide;
  teams: Choice[];
  players: Row[];
  rounds: number[];
}) {
  const update = (k: string, v: string) => set(editCompareSide(q, side, k, v)),
    playerMode = q.view === "Players",
    self = q.player_compare === "self",
    p = `cmp_${side}`;
  const select = (
    key: string,
    label: string,
    choices: Choice[],
    multiple = false,
    placeholder = "All",
  ) => (
    <CompareSelect
      key={key}
      id={`${p}_${key}`}
      label={label}
      value={q[`${side}_${key}`]}
      choices={choices}
      onChange={(v) => update(key, v)}
      multiple={multiple}
      placeholder={placeholder}
    />
  );
  const playerId = q[`player_${side}`],
    teamIds = [
      ...new Set(
        players
          .filter((r) => String(r.player_id) === playerId)
          .map((r) => String(r.team_id)),
      ),
    ];
  const listTeams = q[`list_${side}`].split(",").filter(Boolean),
    pool = players.filter(
      (r) => !listTeams.length || listTeams.includes(String(r.team_id)),
    ),
    map = new Map(pool.map((r) => [String(r.player_id), String(r.name)]));
  return (
    <fieldset className="ibpl-compare-side">
      <legend>
        <span className={`cmp-context-badge ${side}`}>
          {side.toUpperCase()}
        </span>
      </legend>
      {playerMode && self && (
        <CompareDates
          q={q}
          set={(patch) =>
            set({
              ...patch,
              ...(q.auto_players === "true"
                ? { player_a: null, player_b: null, auto_players: "false" }
                : {}),
            })
          }
          rounds={rounds}
          prefix={`${side}_`}
        />
      )}
      {playerMode && (!self || side === "a") && (
        <>
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
                    String(r.player_id) === playerId &&
                    v.split(",").includes(String(r.team_id)),
                )
                  ? { [`player_${side}`]: null }
                  : {}),
              })
            }
          />
          <CompareSelect
            id={`cmp_player_${side}`}
            label="Player"
            placeholder="Search player..."
            value={playerId}
            choices={[...map].map(([value, label]) => ({ value, label }))}
            onChange={(v) =>
              set({
                [`player_${side}`]: v,
                [`player_team_${side}`]: null,
                auto_players: "false",
              })
            }
          />
          {teamIds.length > 1 && (
            <CompareSelect
              id={`cmp_player_${side}_team`}
              label="Team (optional)"
              value={q[`player_team_${side}`]}
              choices={teams.filter((t) => teamIds.includes(t.value))}
              onChange={(v) => set({ [`player_team_${side}`]: v })}
            />
          )}
        </>
      )}
      {!playerMode && (
        <>
          <StarterRows
            p={p}
            query={{
              ...q,
              own_starters: q[`${side}_own_starters`],
              opp_starters: q[`${side}_opp_starters`],
            }}
            set={(patch) => {
              for (const [key, v] of Object.entries(patch))
                update(key, v ?? "");
            }}
          />
          {select("teams", "Teams", teams, true)}
        </>
      )}
      {select("home_away", "Home/Away", [
        { value: "home", label: "Home" },
        { value: "away", label: "Away" },
      ])}
      {select("outcome", "Win/Loss", [
        { value: "win", label: "Win" },
        { value: "loss", label: "Loss" },
      ])}
      <label className="checkbox">
        <input
          type="checkbox"
          id={`${p}_clutch`}
          checked={q[`${side}_clutch`] === "true"}
          onChange={(e) => update("clutch", String(e.target.checked))}
        />{" "}
        Clutch
      </label>
      {q[`${side}_clutch`] === "true" &&
        ["clutch_margin", "clutch_minutes"].map((k) => (
          <label key={k} className="form-group">
            {k === "clutch_margin"
              ? "Max point margin"
              : "Max minutes remaining"}
            : {q[`${side}_${k}`]}
            <input
              id={`${p}_${k}`}
              type="range"
              min={k === "clutch_margin" ? 0 : 1}
              max={k === "clutch_margin" ? 10 : 5}
              step={1}
              value={q[`${side}_${k}`]}
              onChange={(e) => update(k, e.target.value)}
            />
          </label>
        ))}
      {select("opponents", "Opponents", teams, true)}
      {select(
        "game_type",
        "Game type",
        catalogues.gameTypeChoices.filter((c) => c.value !== ""),
        true,
      )}
      <div className="row">
        <div className="col-4">
          {select(
            "opp_rank_side",
            "Top / Bottom",
            [
              { value: "top", label: "Top" },
              { value: "bottom", label: "Bottom" },
            ],
            false,
            "Off",
          )}
        </div>
        <div className="col-4">
          {select(
            "opp_rank_n",
            "Rank N",
            Array.from({ length: 12 }, (_, i) => ({
              value: String(i + 1),
              label: String(i + 1),
            })),
            false,
            "--",
          )}
        </div>
        <div className="col-4">
          {select(
            "opp_rank_metric",
            "Metric",
            [
              { value: "off", label: "Offense" },
              { value: "def", label: "Defense" },
              { value: "net", label: "Net rating" },
            ],
            false,
            "--",
          )}
        </div>
      </div>
    </fieldset>
  );
}
