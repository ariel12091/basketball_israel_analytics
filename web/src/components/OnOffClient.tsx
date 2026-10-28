"use client";
// The On/Off Impact tab: ui_tab1_onoff.R / server_tab1.R (Israeli League) and
// ui_tab8_euro.R / server_tab8_euro.R (EuroLeague, EuroCup). The API returns
// the ranked population of one view; this page applies the team and
// possession filters (onoff_filter_*_rows), auto min-possessions
// (setup_onoff_auto_min), the Summary derivations (3PT luck, sample size),
// the stat filters, and renders the table.
import { useEffect, useMemo, useRef, useState } from "react";
import type { League, Row } from "@/rules/gameLogs";
import {
  autoMinAll,
  autoMinOn,
  onoffTableRows,
  possCols,
  type View,
} from "@/rules/onoff";
import { contextParams, onoffPath } from "@/lib/tabQuery";
import { applyPlayerStatFilters, type PsRow } from "@/rules/playerStats";
import ui from "@/data/shinyUi.json";
import Selectize from "./shiny/Selectize";
import DateRangeInput from "./shiny/DateRangeInput";
import PossStepper from "./shiny/PossStepper";
import ViewTabs from "./shiny/ViewTabs";
import TableLegend, {
  keyFromBlocks,
  moreInfoHint,
  ViewIntro,
} from "./shiny/TableKey";
import OnOffTable from "./shiny/OnOffTables";
import { TableHint, type Block } from "./shiny/Blocks";
import {
  ChipsRow,
  contextChips,
  GameFiltersAccordion,
  list,
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
  ViewContent,
  type DrawerSection,
  type StatFilter,
} from "./shiny/TabKit";

type Payload = {
  rows: Row[];
  league3p: number | null;
  meta: {
    version: string;
    view: View;
    filtered: boolean;
    teams: { id: number; name: string }[];
    rounds: number[];
    phases: string[];
    seasons?: string[];
  };
};
const VIEW_LABEL: Record<View, string> = {
  summary: "Summary",
  four_factors: "Four Factors",
  shot_profile: "Shot Profile",
};
const LABEL_VIEW: Record<string, View> = {
  Summary: "summary",
  "Four Factors": "four_factors",
  "Shot Profile": "shot_profile",
};
const SAMPLE_COLS = ["minutes", "ON Poss", "OFF Poss"]; // ONOFF_SAMPLE_FILTER_COLS
const EMPTY: Row[] = [];

/** onoff_stat_filter_groups(): the column menu's three groups. */
function statGroups(cols: { col: string }[]) {
  const all = cols.map((c) => c.col);
  const ps = ui.onoffStatFilterCols.player_stats.map((c) => c.col);
  return [
    {
      label: "Impact and on/off results",
      cols: all.filter((c) => !SAMPLE_COLS.includes(c) && !ps.includes(c)),
    },
    { label: "Sample size", cols: SAMPLE_COLS.filter((c) => all.includes(c)) },
    { label: "Player Stats", cols: ps.filter((c) => all.includes(c)) },
  ];
}

/** setup_onoff_auto_min(): both bars lower the slider below its default or
 * hand-set value and raise it only to undo their own lowering, capped at that
 * value; a hand move turns auto off, any filter change turns it back on. */
function useAutoMin(args: {
  enabled: boolean;
  base: Row[] | null;
  view: View;
  minAll: number;
  minOn: number;
  setMins: (patch: { min_all?: number; min_on?: number }) => void;
}) {
  const state = useRef({
    lastOn: NaN,
    lastAll: NaN,
    baseOn: NaN,
    baseAll: NaN,
  });
  const { enabled, base, view, minAll, minOn, setMins } = args;
  useEffect(() => {
    if (!enabled || !base) return;
    const cols = possCols(view);
    const s = state.current;
    const step = (
      cur: number,
      needed: number | null,
      last: "lastOn" | "lastAll",
      baseKey: "baseOn" | "baseAll",
    ) => {
      if (needed == null) return null;
      const own = !Number.isNaN(s[last]) && cur === s[last];
      if (cur > needed) {
        if (!own || Number.isNaN(s[baseKey])) s[baseKey] = cur;
        return needed;
      }
      if (!own || Number.isNaN(s[baseKey])) return null;
      const target = Math.min(needed, s[baseKey]);
      return target <= cur ? null : target;
    };
    const onTarget = step(minOn, autoMinOn(base, cols.on), "lastOn", "baseOn");
    // The all-side bar measures the rows that clear the ON bar (Four Factors).
    const gateOn = onTarget ?? minOn;
    const allBase =
      view === "four_factors"
        ? base.filter((r) => Number(r.off_on_poss) >= gateOn)
        : base;
    const allTarget = step(
      minAll,
      autoMinAll(allBase, cols.on, cols.on, cols.off),
      "lastAll",
      "baseAll",
    );
    const patch: { min_all?: number; min_on?: number } = {};
    if (onTarget != null) {
      s.lastOn = onTarget;
      patch.min_on = onTarget;
    }
    if (allTarget != null) {
      s.lastAll = allTarget;
      patch.min_all = allTarget;
    }
    if (Object.keys(patch).length) setMins(patch);
  }, [enabled, base, view, minAll, minOn, setMins]);
  return {
    /** A hand move of a slider: off unless it is the value auto just wrote. */
    isOwn: (which: "on" | "all", v: number) =>
      v === (which === "on" ? state.current.lastOn : state.current.lastAll),
    reset: () =>
      (state.current = {
        lastOn: ui.onoffDefaults.israel.minOn,
        lastAll: ui.onoffDefaults.israel.minAll,
        baseOn: NaN,
        baseAll: NaN,
      }),
  };
}

export default function OnOffClient({
  league,
  initial,
}: {
  league: League;
  initial?: { url: string; data: Payload | null };
}) {
  const euro = league !== "il";
  const p = euro ? "euro" : "on";
  const defaults = euro ? ui.onoffDefaults.euroleague : ui.onoffDefaults.israel;
  const { query, set } = useTabQuery(league, {
    min_all: String(defaults.minAll),
    min_on: String(defaults.minOn),
  });
  const views: View[] = euro
    ? ["summary", "four_factors"]
    : ["summary", "four_factors", "shot_profile"];
  const view: View = views.includes(query.view as View)
    ? (query.view as View)
    : "summary";
  const minAll = Number(query.min_all) || 0,
    minOn = Number(query.min_on) || 0;
  const url = onoffPath(league, query);
  const { data, loading, error, draw, retry, version } = useTabData<Payload>(
    url,
    initial,
  );
  const updated = useVersion(league, version, retry);
  const teams = teamChoices(league, query.season, data?.meta.teams);
  const teamIds = list(query.teams).map(Number);
  const bounds = seasonBounds(league, query.season);
  const allStats = parseStats(query.stats);

  // The table's rows: ranked population -> teams and possession bars ->
  // Summary derivations -> stat filters (ps_* chips need the Player Stats frame).
  const dataView = data?.meta.view ?? view;
  const population = data?.rows ?? EMPTY;
  // The auto-min population: teams filtered, no possession bars (onoff_auto_min_base_df).
  const autoBase = useMemo(() => {
    if (!data) return null;
    if (data.meta.filtered && dataView === "summary") return population;
    return teamIds.length
      ? population.filter((r) => teamIds.includes(Number(r.team_id)))
      : population;
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [data, dataView, query.teams]);
  const [autoOn, setAutoOn] = useState(true);
  const autoKey = `${url}|${query.teams}`;
  const [armedFor, setArmedFor] = useState(autoKey);
  if (armedFor !== autoKey) {
    // Any filter, season or view change re-arms auto (render-time state sync).
    setArmedFor(autoKey);
    setAutoOn(true);
  }
  const auto = useAutoMin({
    enabled: autoOn,
    base: autoBase,
    view: dataView,
    minAll,
    minOn,
    setMins: (patch) =>
      set(
        Object.fromEntries(
          Object.entries(patch).map(([k, v]) => [k, String(v)]),
        ),
      ),
  });

  // Player Stats chips (ps_*) read the Player Stats frame for the same game
  // context, and only while such a chip is active (ps_filter_frame()).
  const psFilters = allStats.filter((f) => f.col.startsWith("ps_"));
  const psActive = psFilters.length > 0;
  const psUrl = useMemo(() => {
    if (!psActive) return null;
    const q = contextParams(query, league);
    for (const k of [
      "view",
      "teams",
      "team",
      "stats",
      "own_starters",
      "opp_starters",
    ])
      q.delete(k);
    return `/api/v1/${league}/player-stat-filters?${q}`;
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [psActive, url, league]);
  const ps = useTabData<{ rows: PsRow[]; meta: { version: string } }>(psUrl);
  const psError = psFilters.length > 0 && Boolean(ps.error);

  const rows = useMemo(
    () =>
      onoffTableRows(
        { rows: population, league3p: data?.league3p ?? null, view: dataView },
        { teams: teamIds, minAll, minOn, filters: allStats },
        psFilters.length
          ? (r) => applyPlayerStatFilters(r, ps.data?.rows ?? [], psFilters)
          : undefined,
      ),
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [
      population,
      dataView,
      query.teams,
      minAll,
      minOn,
      query.stats,
      data?.league3p,
      ps.data,
    ],
  );

  const statCols = [
    ...ui.onoffStatFilterCols[view],
    ...ui.onoffStatFilterCols.player_stats,
  ];
  // A view change keeps the filters the new view still offers and says which
  // it dropped (showNotification in server_tab1.R).
  const [notice, setNotice] = useState<string | null>(null);
  useEffect(() => {
    if (!notice) return;
    const t = setTimeout(() => setNotice(null), 5000);
    return () => clearTimeout(t);
  }, [notice]);
  const changeView = (label: string) => {
    const next = LABEL_VIEW[label];
    const offered = new Set(
      [
        ...ui.onoffStatFilterCols[next],
        ...ui.onoffStatFilterCols.player_stats,
      ].map((c) => c.col),
    );
    const kept = allStats.filter((f) => offered.has(f.col)),
      dropped = allStats.filter((f) => !offered.has(f.col));
    if (dropped.length) {
      const label = (c: string) =>
        statCols.find((x) => x.col === c)?.label ?? c;
      setNotice(
        `Removed filters not available in ${VIEW_LABEL[next]}: ${dropped.map((f) => label(f.col)).join(", ")}`,
      );
    }
    set({
      view: next === "summary" ? null : next,
      stats: kept.length ? JSON.stringify(kept) : null,
    });
  };
  const reset = () => {
    auto.reset();
    setAutoOn(false);
    set({
      teams: null,
      start: null,
      end: null,
      own_starters: null,
      opp_starters: null,
      game_type: null,
      phase: null,
      opponents: null,
      home_away: null,
      outcome: null,
      opp_rank_side: null,
      opp_rank_n: null,
      opp_rank_metric: null,
      min_gn: null,
      max_gn: null,
      last_n: null,
      stats: null,
      min_all: String(defaults.minAll),
      min_on: String(defaults.minOn),
    });
  };
  const chips = contextChips({
    p,
    league,
    query,
    set,
    teams,
    datesAlways: false,
    teamKey: "teams",
    teamFocus: euro ? "euro_teams" : "teams",
    datesFocus: euro ? "euro_date_range" : "date_range",
  });
  const startersActive =
    /^(gte|lte):[0-5]$/.test(query.own_starters) ||
    /^(gte|lte):[0-5]$/.test(query.opp_starters);
  const psNote = `Player Stats use the selected games. Percentages use 0-100.${startersActive ? " Starter-count filters affect On/Off possessions, not Player Stats." : ""}`;
  const frag = euro ? ui.onoff.euro : ui.onoff.il;
  const [rangesOn, setRangesOn] = useState(true);
  const exporter = useRef<((name: string) => void) | null>(null);
  const seasons = euro
    ? (data?.meta.seasons ?? ui.euroStaticSeasons)
    : ui.ilSeasons;
  const start = query.start || bounds.start,
    end = query.end || bounds.end;

  const statFilters = (
    <StatFilters
      p={p}
      cols={statCols}
      groups={statGroups(statCols)}
      filters={allStats}
      hint={psNote}
      onChange={(f: StatFilter[]) =>
        set({ stats: f.length ? JSON.stringify(f) : null })
      }
      extraNote={
        startersActive && allStats.some((f) => f.col.startsWith("ps_")) ? (
          <span className="filter-chip-note text-muted small">
            Player Stats ignore starter-count filters
          </span>
        ) : null
      }
    />
  );
  const drawerSections: DrawerSection[] = [
    {
      id: "dates",
      title: "Dates and teams",
      body: (
        <>
          <DateRangeInput
            id={euro ? "euro_date_range" : "date_range"}
            label="Game Date Range"
            start={start}
            end={end}
            min={bounds.start}
            max={bounds.end}
            onChange={(s, e) => {
              const full = s === bounds.start && e === bounds.end;
              set({ start: full ? null : s, end: full ? null : e });
            }}
          />
          <Selectize
            id={euro ? "euro_teams" : "teams"}
            label="Teams"
            multiple
            placeholder="All teams"
            choices={teams}
            value={list(query.teams)}
            onChange={(v) => set({ teams: v.join(",") })}
          />
        </>
      ),
    },
    {
      id: "starters",
      title: "Lineup starters",
      body: <StarterRows p={p} query={query} set={set} />,
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
  const viewKey = keyFromBlocks(frag.views[view] as Block[]);

  return (
    <TabFrame
      league={league}
      tab="onoff"
      paneValue={euro ? "euro" : "onoff"}
      season={query.season}
      seasons={seasons}
      onSeason={(season) =>
        set({
          season,
          start: null,
          end: null,
          min_gn: null,
          max_gn: null,
          last_n: null,
        })
      }
      views={views.map((v) => VIEW_LABEL[v])}
      view={VIEW_LABEL[view]}
      onView={changeView}
      updated={updated}
      filtersId={`${p === "on" ? "onoff" : "euro"}-filters`}
      layout="drawer"
      sidebar={null}
      drawer={{
        sections: drawerSections,
        activeCount: chips.length,
        // Shiny's always-visible Reset also restored hand-set minimums.
        resettable:
          allStats.length > 0 ||
          (!autoOn && (minAll !== defaults.minAll || minOn !== defaults.minOn)),
        onClearAll: reset,
        resultLabel: `Show ${rows.length} players`,
        footer: (
          <span className="help-block">
            If rows disappear, reduce the possession minimums above the table or
            widen the date range.
          </span>
        ),
      }}
      main={
        <>
          <ChipsRow
            p={p === "on" ? "on" : "euro"}
            seasonChip={seasonChipLabel(league, query.season)}
            chips={chips}
            activeCount={chips.length}
            extra={statFilters}
            active={chips.length > 0 || allStats.length > 0}
            onClearAll={reset}
            controls={
              <>
                <PossStepper
                  id={euro ? "euro_min_all_poss" : "min_all_poss"}
                  label={tt("Min Poss / side", "min_poss_side")}
                  name="Min Poss / side"
                  min={0}
                  max={2000}
                  step={10}
                  value={minAll}
                  onChange={(v) => {
                    if (!auto.isOwn("all", v)) setAutoOn(false);
                    set({ min_all: String(v) });
                  }}
                />
                <PossStepper
                  id={euro ? "euro_min_on_poss" : "min_on_poss"}
                  label={tt("Min ON Poss", "min_on_poss")}
                  name="Min ON Poss"
                  min={0}
                  max={3000}
                  step={10}
                  value={minOn}
                  onChange={(v) => {
                    if (!auto.isOwn("on", v)) setAutoOn(false);
                    set({ min_on: String(v) });
                  }}
                />
                {view === "four_factors" && (
                  <button
                    type="button"
                    className="chips-ranges-toggle js-ranges-toggle"
                    aria-pressed={rangesOn}
                    title="Adds three things to every Four Factors cell: where the player ranks across the league, his on-court and off-court rate, and the estimated points that gap is worth."
                    onClick={() => setRangesOn((o) => !o)}
                  >
                    <i className="bi bi-bar-chart-line" aria-hidden="true"></i>{" "}
                    <span className="js-ranges-toggle-label">
                      {rangesOn ? "Hide on/off detail" : "Show on/off detail"}
                    </span>
                  </button>
                )}
                {!euro && (
                  // downloadButton("download_csv"): Shiny registers no handler
                  // for it; here it exports the visible table.
                  <a
                    className="btn btn-default shiny-download-link"
                    href="#"
                    role="button"
                    onClick={(e) => {
                      e.preventDefault();
                      exporter.current?.(`onoff_${view}`);
                    }}
                  >
                    <i className="bi bi-download" aria-hidden="true"></i>{" "}
                    Download CSV
                  </a>
                )}
              </>
            }
          />
          <ViewTabs
            views={views.map((v) => VIEW_LABEL[v])}
            view={VIEW_LABEL[view]}
            onView={changeView}
          />
          <ViewIntro blocks={frag.views[view] as Block[]} />
          <div className="table-top">
            <TableHint hint={moreInfoHint(frag.hint)} />
            <TableLegend
              shots={viewKey.shots}
              rank={viewKey.rank && (view !== "four_factors" || rangesOn)}
              tags={view === "summary"}
            />
          </div>
          <OutputArea
            id={euro ? "euro_dt" : "onoff_dt"}
            loading={loading || (psFilters.length > 0 && ps.loading)}
            error={error}
            retry={retry}
          >
            {psError ? (
              // PLAYER_STAT_FILTER_ERROR_TEXT, as an Info table.
              <div className="dataTables_wrapper no-footer">
                <table
                  className="display dataTable no-footer"
                  style={{ width: "100%" }}
                >
                  <thead>
                    <tr>
                      <th className="sorting_disabled">Info</th>
                    </tr>
                  </thead>
                  <tbody>
                    <tr className="odd">
                      <td>
                        Player Stats filters could not be evaluated. Remove them
                        or try again.
                      </td>
                    </tr>
                  </tbody>
                </table>
              </div>
            ) : (
              data &&
              (!psFilters.length || ps.data) && (
                <OnOffTable
                  key={`${draw}-${dataView}`}
                  league={league}
                  season={query.season}
                  view={dataView}
                  rows={rows}
                  showImpact={defaults.showImpact}
                  showRanges={rangesOn}
                  exportRef={exporter}
                />
              )
            )}
          </OutputArea>
          {(frag.after[view] as Block[]).length > 0 && (
            <ViewContent blocks={frag.after[view] as Block[]} />
          )}
          {notice && (
            <div id="shiny-notification-panel">
              <div className="shiny-notification shiny-notification-message">
                <div
                  className="shiny-notification-close"
                  role="button"
                  aria-label="Close"
                  onClick={() => setNotice(null)}
                >
                  ×
                </div>
                <div className="shiny-notification-content">
                  <div className="shiny-notification-content-text">
                    {notice}
                  </div>
                </div>
              </div>
            </div>
          )}
        </>
      }
    />
  );
}
