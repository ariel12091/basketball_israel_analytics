"use client";
import ViewTabs from "./shiny/ViewTabs";
import { ViewIntro } from "./shiny/TableKey";
import layout from "@/data/layoutUi.json";
import { useEffect, useMemo, useRef, useState } from "react";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import type { Row } from "@/rules/gameLogs";
import {
  compareDefaults,
  compareSideParams,
  applyPreset,
  resetCompare,
  resolveCompareQuery,
  shortSideLabel,
  fullSideLabel,
  comparePlayerTeams,
  withCompareSelection,
  type CompareSelection,
} from "@/lib/compareQuery";
import {
  useTabQuery,
  TabFrame,
  StatFilters,
  parseStats,
  contextChips,
  ChipsRow,
  Chip,
} from "./shiny/TabKit";
import PossStepper from "./shiny/PossStepper";
import type { Patch } from "@/lib/tabQuery";
import {
  useCompareData,
  CompareStatus,
  type CompareSeed,
} from "./shiny/CompareData";
import CompareSideFilters, {
  CompareSelect,
  CompareDates,
} from "./shiny/CompareFilters";
import {
  EntityDetails,
  TeamPlayerDetail,
  PlayerVersus,
} from "./shiny/CompareDetails";
import DataTable, { type Column } from "./shiny/DataTable";
import { type Block } from "./shiny/Blocks";
import {
  autoCompareMin,
  joinCompare,
  finalizeCompare,
  compareSummary,
  joinTeamPlayers,
  formatCompare,
  num,
  reverseGap,
} from "@/rules/compare";
import data from "@/data/compareUi.json";
import ui from "@/data/shinyUi.json";
import { comparePairReady, compareDefaultPlayers } from "@/lib/compareAsync";
import { seasonLabel } from "./shiny/Navbar";
const EMPTY_ROWS: Row[] = [];
export default function CompareClient({
  initial = {},
  selection,
}: {
  initial?: CompareSeed;
  selection?: CompareSelection;
}) {
  const [client] = useState(() => new QueryClient());
  return (
    <QueryClientProvider client={client}>
      <CompareTab initial={initial} selection={selection} />
    </QueryClientProvider>
  );
}
function CompareTab({
  initial,
  selection,
}: {
  initial: CompareSeed;
  selection?: CompareSelection;
}) {
  const { query: raw, set: patch } = useTabQuery("il", compareDefaults),
    q = withCompareSelection(resolveCompareQuery(raw), selection);
  // Narrow screens show one side at a time (the A | B switch); wide screens
  // show both. Page-local state: it is not a filter.
  const [side, setSide] = useState<"a" | "b">("a");
  const set = (p: Patch) => {
    const changedContext = Object.keys(p).some(
      (k) =>
        /^[ab]_/.test(k) ||
        [
          "start",
          "end",
          "min_gn",
          "max_gn",
          "split_date",
          "split_gn",
          "preset",
          "entity",
        ].includes(k),
    );
    patch({
      ...q,
      ...p,
      ...(changedContext ? { drill_player: null } : {}),
      preset_applied: "true",
    });
  };
  const season = String(q.season),
    modeName = String(q.view),
    aTeams = String(q.a_teams),
    bTeams = String(q.b_teams);
  const refs = useCompareData(
    new URLSearchParams({ season: q.season, kind: "refs" }),
    initial,
  );
  const teams = useMemo(
    () =>
      (
        refs.data?.teams ??
        (ui.ilTeams as Record<string, { id: number; name: string }[]>)[
          season
        ]?.map((t) => ({ team_id: t.id, team_name: t.name })) ??
        []
      ).map((t) => ({ value: String(t.team_id), label: String(t.team_name) })),
    [refs.data?.teams, season],
  );
  const players = refs.data?.players ?? EMPTY_ROWS,
    rounds = refs.data?.rounds ?? [];
  const playerMode = q.view === "Players",
    lineup = q.view === "Lineups",
    teamPlayers = q.cmp_view === "players" && !playerMode;
  const metric = String(q.metric),
    kind =
      playerMode || teamPlayers
        ? "players"
        : lineup
          ? metric.startsWith("off_") && !["off_ppp"].includes(metric)
            ? "lineup_ff"
            : "lineups"
          : metric.startsWith("off_") && metric !== "off_ppp"
            ? "four_factors"
            : "ratings";
  const defaults = useCompareData(
    new URLSearchParams({ season: q.season, kind: "player_defaults" }),
    initial,
    playerMode &&
      refs.isSuccess &&
      q.auto_players === "true" &&
      !q.player_a &&
      !q.player_b,
  );
  const sideParams = (side: "a" | "b") => {
    const s = compareSideParams(q, side, kind);
    if (teamPlayers && q.entity) s.set("teams", q.entity);
    if (playerMode) {
      const ids = comparePlayerTeams(q, side, players);
      if (ids) s.set("teams", ids);
    }
    return s;
  };
  const enabled =
    !playerMode ||
    (refs.isSuccess &&
      !!q.player_a &&
      (q.player_compare === "self" || !!q.player_b) &&
      !!comparePlayerTeams(q, "a", players) &&
      !!comparePlayerTeams(q, "b", players));
  const a = useCompareData(sideParams("a"), initial, enabled),
    b = useCompareData(sideParams("b"), initial, enabled);
  const sameVersion =
    !!a.data && !!b.data && a.data.meta.version === b.data.meta.version;
  const ready = enabled && comparePairReady(a, b);
  const base = useMemo(
    () =>
      !playerMode && !teamPlayers && sameVersion
        ? joinCompare(
            a.data!.rows.filter(
              (r) => !aTeams || aTeams.split(",").includes(String(r.team_id)),
            ),
            b.data!.rows.filter(
              (r) => !bTeams || bTeams.split(",").includes(String(r.team_id)),
            ),
            modeName,
            metric,
            teams,
          )
        : [],
    [
      a.data,
      b.data,
      playerMode,
      teamPlayers,
      sameVersion,
      modeName,
      aTeams,
      bTeams,
      metric,
      teams,
    ],
  );
  const minimum = Math.max(0, Number(q.min_poss) || 0),
    rows = finalizeCompare(
      base,
      q.view,
      q.preset,
      minimum,
      parseStats(q.stats),
    );
  const autoBoot = useRef(false),
    autoContext = useRef(""),
    defaultsChosen = useRef(false);
  const contextKey = `${modeName}|${metric}|${sideParams("a")}|${sideParams("b")}`;
  useEffect(() => {
    if (!ready || playerMode || teamPlayers) return;
    if (!autoBoot.current) {
      autoBoot.current = true;
      autoContext.current = contextKey;
      return;
    }
    if (autoContext.current === contextKey) return;
    autoContext.current = contextKey;
    const next = autoCompareMin(base);
    if (q.auto_min === "true" && next != null && next !== minimum)
      patch({ min_poss: String(next) });
  }, [
    ready,
    playerMode,
    teamPlayers,
    base,
    q.auto_min,
    minimum,
    patch,
    contextKey,
  ]);
  useEffect(() => {
    if (
      !playerMode ||
      !refs.data ||
      !defaults.isSuccess ||
      q.auto_players !== "true" ||
      q.player_a ||
      q.player_b ||
      defaultsChosen.current
    )
      return;
    defaultsChosen.current = true;
    const shuffled = compareDefaultPlayers(
      defaults.data.rows,
      players.map((r) => Number(r.player_id)),
    );
    patch({
      player_a: String(shuffled[0] ?? ""),
      player_b: String(shuffled[1] ?? shuffled[0] ?? ""),
    });
  }, [
    playerMode,
    refs.data,
    defaults.data,
    defaults.isSuccess,
    q.auto_players,
    q.player_a,
    q.player_b,
    players,
    patch,
  ]);
  const reset = () => {
    autoBoot.current = false;
    defaultsChosen.current = false;
    patch(resetCompare());
  };
  const mode = (v: string) => {
    autoBoot.current = false;
    defaultsChosen.current = false;
    patch({ ...resetCompare(), view: v });
  };
  const labels: [string, string] = [
    shortSideLabel(q, "a"),
    shortSideLabel(q, "b"),
  ];
  const fullLabels: [string, string] = [
    fullSideLabel(q, "a", teams),
    fullSideLabel(q, "b", teams),
  ];
  const summaries = compareSummary(rows, q.view, metric),
    title = data.TEAM_METRICS as Record<string, string>,
    metricLabel =
      Object.entries(title).find(([, v]) => v === metric)?.[0] ?? metric;
  const entity = rows.find(
      (r) => String(lineup ? r.sub_lineup_hash : r.team_id) === q.entity,
    ),
    entityName =
      entity?.entity_name ??
      teams.find((t) => t.value === q.entity)?.label ??
      q.entity;
  const open = (r: Row) =>
    set({
      entity: String(lineup ? r.sub_lineup_hash : r.team_id),
      cmp_view: "detail",
      drill_player: null,
    });
  const numeric = (id: string, label: string, signed = false): Column => ({
    id,
    label,
    render: (r) => ({
      node: formatCompare(r[id], "num", signed),
      text: formatCompare(r[id], "num", signed),
    }),
  });
  const columns: Column[] = [
    { id: "rank", label: "#" },
    {
      id: "entity_name",
      label: lineup ? "Lineup" : "Team",
      onActivate: (r) => open(r),
      tdProps: () => ({ role: "button", tabIndex: 0 }),
    },
    ...(lineup ? [{ id: "team_name", label: "Team" }] : []),
    numeric("metric_a", labels[0]),
    {
      id: "poss_a",
      label: "Total Poss A",
      render: (r) => ({
        node: Math.trunc(num(r.poss_a)),
        text: String(Math.trunc(num(r.poss_a))),
      }),
    },
    numeric("metric_b", labels[1]),
    {
      id: "poss_b",
      label: "Total Poss B",
      render: (r) => ({
        node: Math.trunc(num(r.poss_b)),
        text: String(Math.trunc(num(r.poss_b))),
      }),
    },
    { ...numeric("gap", "Gap", true), descFirst: true, descOnly: true },
  ];
  const playerRows =
    ready && teamPlayers
      ? joinTeamPlayers(
          a.data!.rows,
          b.data!.rows,
          q.team_metric,
          q.team_rate,
          minimum,
          reverseGap(q.preset),
        )
      : [];
  const playerColumns: Column[] = [
    { id: "rank", label: "#" },
    {
      id: "player_name",
      label: "Player",
      onActivate: (r) => set({ drill_player: String(r.player_id) }),
      tdProps: () => ({ role: "button", tabIndex: 0 }),
    },
    ...(["a", "b"] as const).flatMap((side, i): Column[] => [
      {
        id: `gp_${side}`,
        label: `GP ${side.toUpperCase()}`,
        render: (r) => ({
          node: r[`played_${side}`] ? r[`gp_${side}`] : 0,
          text: String(r[`played_${side}`] ? r[`gp_${side}`] : 0),
        }),
      },
      {
        ...numeric(`metric_${side}`, labels[i]),
        render: (r) => {
          const text = `${formatCompare(r[`metric_${side}`], (data.TEAM_PLAYER_PCT_COLS as string[]).includes(q.team_metric) ? "pct" : "num")}${r[`played_${side}`] ? "" : " (didn't play)"}`;
          return { node: text, text };
        },
      },
      { id: `poss_${side}`, label: `Total Poss ${side.toUpperCase()}` },
    ]),
    {
      ...numeric("gap", "Gap", true),
      render: (r) => {
        const text = formatCompare(
          r.gap,
          (data.TEAM_PLAYER_PCT_COLS as string[]).includes(q.team_metric)
            ? "pct"
            : "num",
          true,
        );
        return { node: text, text };
      },
      sortValue: (r) => r.gap_score,
      descFirst: true,
      descOnly: true,
    },
  ];
  const selectedPlayer = playerRows.find(
    (r) => String(r.player_id) === q.drill_player,
  );
  const playerId = (s: "a" | "b") =>
    q[`player_${q.player_compare === "self" ? "a" : s}`];
  const playerRow = (side: "a" | "b") =>
    (side === "a" ? a : b).data?.rows.find(
      (r) => String(r.player_id) === playerId(side),
    ) ?? null;
  const playerNames = (["a", "b"] as const).map((s) =>
    String(
      players.find((r) => String(r.player_id) === playerId(s))?.name ?? "",
    ),
  ) as [string, string];
  const playerTeams = (["a", "b"] as const).map((s) =>
    String(
      teams.find(
        (t) => t.value === comparePlayerTeams(q, s, players).split(",")[0],
      )?.label ?? "",
    ),
  ) as [string, string];
  const metricButtons = (
    map: Record<string, string>,
    value: string,
    onChange: (v: string) => void,
  ) => (
    <div
      className="ibpl-compare-metrics"
      role="group"
      aria-label="Comparison metric"
    >
      {Object.entries(map).map(([label, v]) => (
        <button
          key={v}
          type="button"
          aria-pressed={value === v}
          className={`btn btn-sm ${value === v ? "btn-warning" : "btn-outline-secondary"}`}
          onClick={() => onChange(v)}
        >
          {label}
        </button>
      ))}
    </div>
  );
  const rateButtons = (values: string[], value: string, key: string) => (
    <div className="ibpl-compare-metrics" role="group" aria-label="Rate">
      {values.map((v) => (
        <button
          key={v}
          type="button"
          aria-pressed={value === v}
          className={`btn btn-sm ${value === v ? "btn-warning" : "btn-outline-secondary"}`}
          onClick={() => set({ [key]: v })}
        >
          {v}
        </button>
      ))}
    </div>
  );
  const stats = Object.entries(data.filterColumns).map(([label, col]) => ({
    label,
    col,
  }));
  const chips = contextChips({
    p: "cmp",
    league: "il",
    query: {
      ...q,
      ...(q.preset === "date_split" ? { start: "", end: "" } : {}),
      ...(q.preset === "gn_split" ? { min_gn: "", max_gn: "" } : {}),
    },
    set,
    teams,
    datesAlways: false,
    teamKey: "teams",
    teamFocus: "cmp_a_teams",
    datesFocus: "cmp_dates",
  });
  // The settings both sides share sit in a bar under the view tabs, the quick
  // preset first and largest: it is this page's most used control (user
  // 2026-10-28). No Filters panel: dates and game numbers are too few for one.
  const shared = (
    <>
      {!playerMode ? (
        <>
          <div className="cmp-preset">
            <CompareSelect
              id="cmp_preset"
              label="Quick preset"
              value={q.preset}
              choices={data.presetChoices}
              placeholder="-- Custom --"
              onChange={(v) => set(applyPreset(q, v))}
            />
          </div>
          {q.preset === "date_split" ? (
            <label>
              Split date
              <input
                id="cmp_split_date"
                className="form-control"
                type="date"
                value={q.split_date}
                onChange={(e) =>
                  set({ split_date: e.target.value, auto_min: "true" })
                }
              />
            </label>
          ) : null}
          {q.preset === "gn_split" ? (
            <CompareSelect
              id="cmp_split_gn"
              label="Split GN"
              value={q.split_gn}
              choices={rounds.map((n) => ({
                value: String(n),
                label: String(n),
              }))}
              onChange={(v) => set({ split_gn: v, auto_min: "true" })}
            />
          ) : null}
          <CompareDates
            q={q}
            set={set}
            rounds={rounds}
            showDates={q.preset !== "date_split"}
            showGN={q.preset !== "gn_split"}
          />
        </>
      ) : (
        <>
          <CompareSelect
            id="cmp_player_compare_mode"
            label="Compare"
            value={q.player_compare}
            choices={[
              { value: "other", label: "Other players" },
              { value: "self", label: "Against himself" },
            ]}
            onChange={(v) => set({ player_compare: v })}
          />
          {q.player_compare !== "self" && (
            <CompareDates
              q={q}
              set={(p) =>
                set({
                  ...p,
                  ...(q.auto_players === "true"
                    ? { player_a: null, player_b: null, auto_players: "false" }
                    : {}),
                })
              }
              rounds={rounds}
            />
          )}
        </>
      )}
    </>
  );
  const sides = (
    <>
      <div className="cmp-side-switch" role="group" aria-label="Show side">
        {(["a", "b"] as const).map((s) => (
          <button
            key={s}
            type="button"
            aria-pressed={side === s}
            className={side === s ? "is-active" : ""}
            onClick={() => setSide(s)}
          >
            Side {s.toUpperCase()}
          </button>
        ))}
      </div>
      {(["a", "b"] as const).map((s) => (
        <div key={s} className={`cmp-side${side === s ? " is-shown" : ""}`}>
          {s === "b" && (
            <div className="cmp-vs text-center text-muted fw-bold my-2">
              -- vs --
            </div>
          )}
          <CompareSideFilters
            q={q}
            set={set}
            side={s}
            teams={teams}
            players={players}
            rounds={rounds}
          />
        </div>
      ))}
    </>
  );
  const main = (
    <>
      <ChipsRow
        p="cmp"
        seasonChip={seasonLabel(q.season)}
        active={!!q.stats || chips.length > 0}
        onClearAll={reset}
        chips={playerMode ? [] : chips}
        activeCount={playerMode ? 0 : chips.length}
        controls={
          !playerMode && (
            <>
              <PossStepper
                id="cmp_min_poss"
                label="Min Poss"
                name="Min possessions"
                min={0}
                max={Math.max(2000, minimum)}
                step={10}
                value={minimum}
                onChange={(v) =>
                  set({ min_poss: String(v), auto_min: "false" })
                }
              />
              <StatFilters
                p="cmp"
                cols={stats}
                filters={parseStats(q.stats)}
                onChange={(v) => set({ stats: JSON.stringify(v) })}
              />
            </>
          )
        }
        extra={
          !playerMode && (
            <>
              {q.stats &&
                parseStats(q.stats).map((f, i) => (
                  <Chip
                    key={i}
                    label={`${stats.find((s) => s.col === f.col)?.label ?? f.col} ${f.op === "ge" ? ">=" : "<="} ${f.value}`}
                    onClear={() =>
                      set({
                        stats: JSON.stringify(
                          parseStats(q.stats).filter((_, j) => i !== j),
                        ),
                      })
                    }
                  />
                ))}
            </>
          )
        }
      />
      <ViewTabs
        views={["Teams", "Lineups", "Players"]}
        view={q.view}
        onView={mode}
      />
      <div className="cmp-settings" role="group" aria-label="Compare settings">
        {shared}
      </div>
      <div className="table-top">
        <ViewIntro
          blocks={ui.compare.blocks as Block[]}
          summary={layout.intro.compare}
        />
      </div>
      <div className="cmp-layout">
        <section className="cmp-sides" aria-label="Sides A and B">
          {sides}
        </section>
        <div className="cmp-results">
          {!playerMode && q.preset === "date_split" && !q.split_date && (
            <p role="alert">
              {data.text.find(
                (t) =>
                  typeof t === "string" &&
                  t.startsWith("Date split preset needs"),
              )}
            </p>
          )}
          <CompareStatus label="Reference data" query={refs} />
          {playerMode && (
            <CompareStatus label="Default players" query={defaults} />
          )}
          {!playerMode && (
            <div
              className="ibpl-compare-metrics"
              role="group"
              aria-label="Comparison view"
            >
              {[
                "league",
                "detail",
                ...(q.entity && q.view === "Teams" ? ["players"] : []),
              ].map((v) => (
                <button
                  key={v}
                  type="button"
                  className={`btn btn-sm ${q.cmp_view === v ? "btn-warning" : "btn-outline-secondary"}`}
                  onClick={() =>
                    set({
                      cmp_view: v,
                      ...(v === "league"
                        ? { entity: null, drill_player: null }
                        : {}),
                    })
                  }
                >
                  {v[0].toUpperCase() + v.slice(1)}
                </button>
              ))}
              {q.cmp_view === "detail" && (
                <CompareSelect
                  id="cmp_detail_entity_select"
                  label="Entity"
                  placeholder="Select entity..."
                  value={q.entity}
                  choices={rows.map((r) => ({
                    value: String(lineup ? r.sub_lineup_hash : r.team_id),
                    label: r.entity_name,
                  }))}
                  onChange={(v) => set({ entity: v })}
                />
              )}
            </div>
          )}
          <div className="ibpl-compare-side-status">
            <CompareStatus label="Side A" query={a} />
            <CompareStatus label="Side B" query={b} />
          </div>
          {a.data && b.data && !sameVersion && (
            <div role="alert">
              The data changed between sides.{" "}
              <button
                type="button"
                onClick={() => {
                  void a.refetch();
                  void b.refetch();
                }}
              >
                Refresh both sides
              </button>
            </div>
          )}
          {playerMode ? (
            <>
              {metricButtons(
                data.PLAYER_VIEWS as Record<string, string>,
                q.player_view,
                (v) => set({ player_view: v }),
              )}
              {q.player_view !== "ff_swing" &&
                rateButtons(
                  ["Per Game", "Per 75 Possessions", "Totals"],
                  q.rate,
                  "rate",
                )}
              {!enabled ? (
                <p>
                  Select{" "}
                  {q.player_compare === "self"
                    ? "a player"
                    : "Player A and Player B"}{" "}
                  to run Players compare.
                </p>
              ) : (
                ready && (
                  <PlayerVersus
                    q={q}
                    seed={initial}
                    a={playerRow("a")}
                    b={playerRow("b")}
                    names={playerNames}
                    teams={playerTeams}
                    teamIds={[
                      sideParams("a").get("teams") ?? "",
                      sideParams("b").get("teams") ?? "",
                    ]}
                    version={a.data!.meta.version}
                    refresh={() => Promise.all([a.refetch(), b.refetch()])}
                  />
                )
              )}
            </>
          ) : q.cmp_view === "detail" ? (
            q.entity ? (
              <>
                <button
                  type="button"
                  className="cmp-back-btn"
                  onClick={() => set({ cmp_view: "league", entity: null })}
                >
                  &lt;- Back to league view
                </button>
                <EntityDetails
                  q={q}
                  seed={initial}
                  name={entityName}
                  labels={labels}
                  descriptions={fullLabels}
                />
              </>
            ) : (
              <p>
                Select a team or lineup from the dropdown to view detailed
                comparison.
              </p>
            )
          ) : teamPlayers ? (
            <>
              {metricButtons(
                data.TEAM_PLAYER_METRICS as Record<string, string>,
                q.team_metric,
                (v) => set({ team_metric: v }),
              )}
              {rateButtons(
                ["Per Game", "Per 60 Possessions", "Per 30 Minutes", "Totals"],
                q.team_rate,
                "team_rate",
              )}
              <button
                type="button"
                className="cmp-back-btn"
                onClick={() =>
                  set(
                    selectedPlayer
                      ? { drill_player: null }
                      : { cmp_view: "detail" },
                  )
                }
              >
                &lt;- Back to {selectedPlayer ? "players" : "team detail"}
              </button>
              {selectedPlayer ? (
                <TeamPlayerDetail row={selectedPlayer} q={q} labels={labels} />
              ) : (
                ready && (
                  <DataTable
                    columns={playerColumns}
                    rows={playerRows}
                    defaultOrder={[{ id: "gap", desc: true }]}
                    paging={false}
                    controls={false}
                    nameColumns={["player_name"]}
                    moreInfo
                    onRowActivate={(r) =>
                      set({ drill_player: String(r.player_id) })
                    }
                    rowProps={() => ({ className: "ibpl-compare-row" })}
                  />
                )
              )}
            </>
          ) : (
            <>
              {metricButtons(title, metric, (v) => set({ metric: v }))}
              <div className="row cmp-summary-row">
                {(["a", "b"] as const).map((side, i) => (
                  <div className="col-sm-4" key={side}>
                    <div className="card bg-dark border-secondary p-3 mb-3">
                      <div className="small text-uppercase">
                        {fullLabels[i]}
                      </div>
                      <span
                        className={`cmp-side-value ${summaries.winner === "tie" ? "is-tie" : summaries.winner === side ? "is-winner" : "is-loser"}`}
                      >
                        {ready ? formatCompare(summaries[side]) : "--"}
                      </span>
                      <div className="small text-muted">{metricLabel}</div>
                      <div className="small text-muted">
                        Delta vs {side === "a" ? "B" : "A"}:{" "}
                        {ready
                          ? formatCompare(
                              num(summaries.delta) * (i ? -1 : 1),
                              "num",
                              true,
                            )
                          : "--"}
                      </div>
                      <div className="small text-muted">
                        Poss {side.toUpperCase()}:{" "}
                        {ready
                          ? formatCompare(summaries[`poss_${side}`], "int")
                          : "--"}
                      </div>
                    </div>
                  </div>
                ))}
                <div className="col-sm-4">
                  <div className="card bg-dark border-secondary p-3 mb-3">
                    <div>Avg Gap</div>
                    <div className="fs-4 fw-bold">
                      {ready ? formatCompare(summaries.gap_abs) : "--"}
                    </div>
                    <div className="small text-muted">league-wide</div>
                  </div>
                </div>
              </div>
              {lineup && (
                <div className="ibpl-compare-lineup-controls">
                  <CompareSelect
                    id="cmp_lu_num"
                    label="Size"
                    value={q.size}
                    choices={[2, 3, 4, 5].map((n) => ({
                      value: String(n),
                      label: String(n),
                    }))}
                    onChange={(v) => set({ size: v, auto_min: "true" })}
                  />
                  <CompareSelect
                    id="cmp_lu_filter_team"
                    label="Team"
                    value={q.lu_team}
                    choices={teams}
                    onChange={(v) =>
                      set({
                        lu_team: v,
                        lu_on: null,
                        lu_any: null,
                        lu_off: null,
                        auto_min: "true",
                      })
                    }
                  />
                  {q.lu_team &&
                    ["on", "any", "off"].map((k) => (
                      <CompareSelect
                        key={k}
                        id={`cmp_lu_filter_${k}`}
                        label={
                          k === "on" ? "All of" : k === "any" ? "Any of" : "Off"
                        }
                        value={q[`lu_${k}`]}
                        choices={players
                          .filter((r) => String(r.team_id) === q.lu_team)
                          .map((r) => ({
                            value: String(r.player_id),
                            label: String(r.name),
                          }))}
                        multiple
                        onChange={(v) =>
                          set({ [`lu_${k}`]: v, auto_min: "true" })
                        }
                      />
                    ))}
                </div>
              )}
              {ready &&
                (rows.length ? (
                  <DataTable
                    columns={columns}
                    rows={rows}
                    defaultOrder={[]}
                    paging={false}
                    controls={false}
                    moreInfo
                    nameColumns={
                      lineup ? ["entity_name", "team_name"] : ["entity_name"]
                    }
                    tableClass="compact stripe nowrap"
                    onRowActivate={open}
                    rowProps={() => ({ className: "ibpl-compare-row" })}
                  />
                ) : (
                  <p>No comparisons match the current filters.</p>
                ))}
            </>
          )}
        </div>
      </div>
    </>
  );
  return (
    <TabFrame
      league="il"
      tab="compare"
      paneValue="compare"
      season={q.season}
      seasons={ui.ilSeasons}
      onSeason={(s) => {
        autoBoot.current = false;
        defaultsChosen.current = false;
        patch({ ...resetCompare(), season: s, view: q.view });
      }}
      views={["Teams", "Lineups", "Players"]}
      view={q.view}
      onView={mode}
      updated={refs.data?.meta.version ?? null}
      filtersId="cmp-filters"
      mainOnly
      sidebarTop={null}
      sidebar={null}
      main={main}
    />
  );
}
