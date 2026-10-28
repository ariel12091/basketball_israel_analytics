"use client";
// Tabs 2/10, using the same URL state, frame, filters and table as other tabs.
import { useEffect, useMemo, useState } from "react";
import type { League, Row } from "@/rules/gameLogs";
import {
  playerIds,
  type PlayerSelection,
  type LineupView,
} from "@/rules/lineups";
import {
  LINEUP_QUERY_DEFAULTS,
  lineupsPagePath,
  lineupsPath,
  queryDefaults,
} from "@/lib/tabQuery";
import ui from "@/data/shinyUi.json";
import { type Block } from "./shiny/Blocks";
import layout from "@/data/layoutUi.json";
import Selectize from "./shiny/Selectize";
import DateRangeInput from "./shiny/DateRangeInput";
import PossStepper from "./shiny/PossStepper";
import ViewTabs from "./shiny/ViewTabs";
import TableLegend, { keyFromBlocks, ViewIntro } from "./shiny/TableKey";
import LineupPlayers from "./shiny/LineupPlayers";
import LineupTable from "./shiny/LineupTable";
import LineupGameLog from "./shiny/LineupGameLog";
import {
  ChipsRow,
  Chip,
  contextChips,
  GameFiltersAccordion,
  OutputArea,
  parseStats,
  seasonBounds,
  seasonChipLabel,
  StarterRows,
  StatFilters,
  TabFrame,
  teamChoices,
  tt,
  useTabData,
  useTabQuery,
  useVersion,
  type DrawerSection,
} from "./shiny/TabKit";

type Payload = {
  rows: Row[];
  meta: {
    version: string;
    teams: { id: number; name: string }[];
    rounds: number[];
    phases: string[];
    seasons?: string[];
    // The table's page state, applied on the server (lineupsPageAnswer).
    autoMin: number | null;
    minPoss: number;
    shown: number;
    maxPoss: number;
  };
};
/** The same lineup row (team and lineup hash). */
const same = (x: Row, y: Row) =>
  x.team_id === y.team_id && x.sub_lineup_hash === y.sub_lineup_hash;
const EMPTY: Row[] = [];
const LABELS = { summary: "Summary", four_factors: "Four Factors" };
export default function LineupsClient({
  league,
  initial,
}: {
  league: League;
  initial?: { url: string; data: Payload | null };
}) {
  const euro = league !== "il",
    p = euro ? "euro_ld" : "ld";
  const { query, set } = useTabQuery(league, {
    ...LINEUP_QUERY_DEFAULTS,
    min_poss: euro ? "0" : String(ui.lineupDefaults.minPoss),
  });
  const view: LineupView =
    query.view === "four_factors" ? "four_factors" : "summary";
  // The population's address (re-arms auto) and the table's (what is fetched:
  // team/player filters, the minimum and stat filters run on the server).
  const populationUrl = lineupsPath(league, query);
  const [auto, setAuto] = useState(true);
  const url = lineupsPagePath(league, query, auto);
  const { data, loading, error, draw, retry, version } = useTabData<Payload>(
    url,
    initial,
  );
  const updated = useVersion(league, version, retry);
  const rosterUrl = `/api/v1/${league}/lineup-roster?season=${query.season}`;
  const rosterData = useTabData<{ rows: Row[]; meta: { version: string } }>(
    rosterUrl,
  );
  const teams = teamChoices(league, query.season, data?.meta.teams);
  const roster = useMemo(
    () =>
      (rosterData.data?.rows ?? EMPTY)
        .filter((r) => String(r.team_id) === query.team)
        .sort(
          (a, b) =>
            Number(b.minutes ?? -1) - Number(a.minutes ?? -1) ||
            String(a.name).localeCompare(String(b.name)),
        ),
    [rosterData.data, query.team],
  );
  const selection = useMemo<PlayerSelection>(
    () => ({
      on: playerIds(query.players_on),
      any: playerIds(query.players_any),
      off: playerIds(query.players_off),
      count: Number(query.any_count) || 1,
      exact: query.any_exact === "true",
    }),
    [
      query.players_on,
      query.players_any,
      query.players_off,
      query.any_count,
      query.any_exact,
    ],
  );
  const setPlayers = (s: PlayerSelection) =>
    set({
      players_on: s.on.join(","),
      players_any: s.any.join(","),
      players_off: s.off.join(","),
      any_count: String(s.count),
      any_exact: String(s.exact),
    });
  const autoKey = JSON.stringify([
    populationUrl,
    query.team,
    query.players_on,
    query.players_any,
    query.players_off,
    query.any_count,
    query.any_exact,
  ]);
  const [armed, setArmed] = useState(autoKey);
  if (armed !== autoKey) {
    setArmed(autoKey);
    setAuto(true);
  }
  // Auto's value comes with the answer for this page state; written to the
  // slider it does not change the auto address, so it costs no request.
  const target =
      auto && data?.meta.autoMin !== undefined ? data.meta.autoMin : null,
    min = Number(query.min_poss) || 0;
  useEffect(() => {
    if (auto && target != null && target !== min)
      set({ min_poss: String(target) });
  }, [auto, target, min, set]);
  const filters = parseStats(query.stats);
  const rows = data?.rows ?? EMPTY;
  const [clicked, setClicked] = useState<Row | null>(null);
  const bounds = seasonBounds(league, query.season);
  const frag = euro ? ui.lineups.euro : ui.lineups.il;
  const changeView = (label: string) =>
    set({
      view: label === "Four Factors" ? "four_factors" : "summary",
      ...(!euro ? { stats: null } : {}),
    });
  const clearPlayers = {
    players_on: null,
    players_any: null,
    players_off: null,
    any_count: null,
    any_exact: null,
  };
  const reset = () => {
    // Tab 10 retains its current view/stat filters and re-arms auto. Tab 2
    // resets to Summary and the fixed default 20 until another filter changes.
    const defaults = queryDefaults(league, {
      ...LINEUP_QUERY_DEFAULTS,
      min_poss: euro ? "0" : String(ui.lineupDefaults.minPoss),
    });
    const next = {
      ...Object.fromEntries(Object.keys(defaults).map((k) => [k, null])),
      season: query.season,
      ...(euro ? { view: query.view, stats: query.stats } : {}),
      min_poss: euro ? "0" : String(ui.lineupDefaults.minPoss),
    };
    setArmed(
      JSON.stringify([
        lineupsPath(league, {
          ...defaults,
          season: query.season,
          ...(euro ? { view: query.view } : {}),
        }),
        "",
        "",
        "",
        "",
        "1",
        "false",
      ]),
    );
    setAuto(euro);
    set(next);
  };
  const chips = contextChips({
    p,
    league,
    query: { ...query, teams: query.team },
    set: (patch) =>
      set({
        ...patch,
        ...("teams" in patch ? { team: patch.teams, ...clearPlayers } : {}),
      }),
    teams,
    datesAlways: !euro,
    teamKey: "teams",
    teamFocus: `${p}_team`,
    datesFocus: `${p}_dates`,
  });
  for (const [key, label] of [
    ["players_on", "On"],
    ["players_any", "Any of"],
    ["players_off", "Off"],
  ])
    if (query[key])
      chips.push(
        <Chip
          key={key}
          label={`${label}: ${playerIds(query[key])
            .map(
              (id) =>
                roster.find((r) => Number(r.player_id) === id)?.name ?? id,
            )
            .join(", ")}`}
          onClear={() => set({ [key]: null })}
        />,
      );
  if (query.clutch === "true")
    chips.push(
      <Chip
        key="clutch"
        label={`Clutch: ${query.clutch_margin} pts / ${query.clutch_minutes} min`}
        onClear={() => set({ clutch: null })}
      />,
    );
  const viewKey = keyFromBlocks(frag.views[view] as Block[]);
  // The Israeli tab always shows its date chip (Shiny); it is not a filter
  // until the dates move off the season's bounds.
  const activeCount =
    chips.length - (!euro && !query.start && !query.end ? 1 : 0);
  const drawerSections: DrawerSection[] = [
    {
      id: "lineup",
      title: "Lineup",
      body: (
        <>
          <div
            className="form-group shiny-input-radiogroup shiny-input-container shiny-input-container-inline"
            role="radiogroup"
            aria-label="Group size"
          >
            <label className="control-label">
              {tt("Group size", "group_size")}
            </label>
            <div className="shiny-options-group">
              {["2", "3", "4", "5"].map((n) => (
                <label key={n} className="radio-inline">
                  <input
                    type="radio"
                    name={`${p}_num`}
                    value={n}
                    checked={query.size === n}
                    onChange={() => set({ size: n })}
                  />
                  <span>{n}</span>
                </label>
              ))}
            </div>
          </div>
          <Selectize
            id={`${p}_team`}
            label="Team"
            choices={teams}
            value={query.team ? [query.team] : []}
            onChange={(v) => set({ team: v[0] ?? null, ...clearPlayers })}
            emptyLabel="- All teams -"
            placeholder="All teams"
          />
          {rosterData.error ? (
            <div role="alert">
              Player roster unavailable.{" "}
              <button type="button" onClick={rosterData.retry}>
                Retry
              </button>
            </div>
          ) : (
            <LineupPlayers
              key={`${league}-${query.season}-${query.team}`}
              roster={roster}
              selection={selection}
              onChange={setPlayers}
            />
          )}
        </>
      ),
    },
    {
      id: "starters",
      title: "Lineup starters",
      body: <StarterRows p={p} query={query} set={set} />,
    },
    {
      id: "dates",
      title: "Dates and clutch",
      body: (
        <>
          <DateRangeInput
            id={`${p}_dates`}
            label="Date range"
            start={query.start || bounds.start}
            end={query.end || bounds.end}
            min={bounds.start}
            max={bounds.end}
            onChange={(start, end) => set({ start, end })}
          />
          <div className="checkbox">
            <label>
              <input
                type="checkbox"
                aria-label="Clutch"
                checked={query.clutch === "true"}
                onChange={(e) => set({ clutch: String(e.target.checked) })}
              />
              {tt("Clutch", "clutch")}
            </label>
          </div>
          {query.clutch === "true" && (
            <>
              <PossStepper
                id={`${p}_clutch_margin`}
                label="Max point margin"
                name="Max point margin"
                min={0}
                max={10}
                step={1}
                value={Number(query.clutch_margin)}
                onChange={(v) => set({ clutch_margin: String(v) })}
              />
              <Selectize
                id={`${p}_clutch_status`}
                label="Score status"
                choices={[
                  { value: "all", label: "All" },
                  { value: "leading", label: "Leading" },
                  { value: "trailing", label: "Trailing" },
                  { value: "tied", label: "Tied" },
                ]}
                value={[query.clutch_status]}
                onChange={(v) => set({ clutch_status: v[0] ?? "all" })}
              />
              <PossStepper
                id={`${p}_clutch_minutes`}
                label="Max minutes remaining"
                name="Max minutes remaining"
                min={1}
                max={5}
                step={1}
                value={Number(query.clutch_minutes)}
                onChange={(v) => set({ clutch_minutes: String(v) })}
              />
              <div className="checkbox">
                <label>
                  <input
                    type="checkbox"
                    checked={query.ot_margin === "true"}
                    onChange={(e) =>
                      set({ ot_margin: String(e.target.checked) })
                    }
                  />
                  Exclude OT if margin exceeded
                </label>
              </div>
              <span className="help-block">
                By default, overtime always qualifies. Check above to apply
                margin filter to OT.
              </span>
            </>
          )}
        </>
      ),
    },
    {
      id: "game",
      title: "Game filters",
      body: (
        <GameFiltersAccordion
          p={p}
          league={league}
          query={query}
          set={set}
          teams={teams}
          rounds={data?.meta.rounds ?? []}
          phases={data?.meta.phases ?? []}
          oppRank
          bare
        />
      ),
    },
  ];
  return (
    <TabFrame
      league={league}
      tab="lineup_data"
      paneValue={euro ? "euro_lineups" : "lineup_data"}
      season={query.season}
      seasons={
        euro ? (data?.meta.seasons ?? ui.euroStaticSeasons) : ui.ilSeasons
      }
      onSeason={(season) => {
        setClicked(null);
        set({
          season,
          start: null,
          end: null,
          team: null,
          ...clearPlayers,
          min_gn: null,
          max_gn: null,
          last_n: null,
        });
      }}
      views={Object.values(LABELS)}
      view={LABELS[view]}
      onView={changeView}
      updated={updated}
      filtersId={`${p}-filters`}
      layout="drawer"
      sidebar={null}
      drawer={{
        sections: drawerSections,
        activeCount: activeCount,
        // Shiny's always-visible Reset also restored a hand-set minimum.
        resettable: filters.length > 0 || !auto,
        onClearAll: reset,
        resultLabel: `Show ${rows.filter((r) => r.is_total !== 0).length} lineups`,
        footer: (
          <span className="help-block">
            The possession minimum sits above the table. Higher minimums improve
            stability but remove smaller-sample lineups.
          </span>
        ),
      }}
      main={
        <>
          <ChipsRow
            p={p}
            seasonChip={seasonChipLabel(league, query.season)}
            chips={chips}
            activeCount={activeCount}
            active={chips.length > 0 || filters.length > 0}
            onClearAll={reset}
            extra={
              <>
                <StatFilters
                  p={p}
                  cols={ui.lineupStatFilterCols[view]}
                  filters={filters}
                  onChange={(fs) =>
                    set({ stats: fs.length ? JSON.stringify(fs) : null })
                  }
                />
                {euro && (
                  <span className="filter-chip">
                    {query.size}-player units / {data?.meta.shown ?? 0} shown
                    {min > 0
                      ? ` / min ${min} poss${auto ? " (auto)" : ""}`
                      : ""}
                  </span>
                )}
              </>
            }
            controls={
              <PossStepper
                id={`${p}_minposs`}
                label={tt("Min Poss", "min_poss_lineup")}
                name="Min Poss"
                min={0}
                max={euro ? (data?.meta.maxPoss ?? 10) : 2000}
                step={10}
                value={min}
                onChange={(v) => {
                  setAuto(false);
                  set({ min_poss: String(v) });
                }}
              />
            }
          />
          <ViewTabs
            views={Object.values(LABELS)}
            view={LABELS[view]}
            onView={changeView}
          />
          <div className="table-top">
            <ViewIntro
              blocks={frag.views[view] as Block[]}
              summary={layout.intro.lineups[view]}
            />
            <TableLegend
              league={league}
              shots={viewKey.shots}
              rank={viewKey.rank}
            />
          </div>
          <OutputArea
            id={euro ? "euro_ld_dt" : "ld_table"}
            loading={loading}
            error={error}
            retry={retry}
          >
            <LineupTable
              key={JSON.stringify([
                draw,
                view,
                query.team,
                selection,
                min,
                query.stats,
              ])}
              rows={rows}
              view={view}
              // A lineup's game log opens in place beneath its row; a second
              // click on the same lineup folds it away.
              onLineup={(r) => setClicked((c) => (c && same(c, r) ? null : r))}
              detail={(r) =>
                clicked && same(clicked, r) ? (
                  <LineupGameLog
                    league={league}
                    season={query.season}
                    row={clicked}
                    view={view}
                    onClose={() => setClicked(null)}
                  />
                ) : null
              }
            />
          </OutputArea>
        </>
      }
    />
  );
}
