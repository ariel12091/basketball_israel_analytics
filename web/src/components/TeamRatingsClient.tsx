"use client";
import { useMemo } from "react";
import type { League, Row } from "@/rules/gameLogs";
import { teamTableRows, type TeamView } from "@/rules/teamRatings";
import {
  TEAM_QUERY_DEFAULTS,
  teamRatingsPath,
  queryDefaults,
} from "@/lib/tabQuery";
import ui from "@/data/shinyUi.json";
import TeamRatingsTable from "./shiny/TeamRatingsTable";
import ClutchFilters from "./shiny/ClutchFilters";
import Selectize from "./shiny/Selectize";
import DateRangeInput from "./shiny/DateRangeInput";
import { type Block } from "./shiny/Blocks";
import ViewTabs from "./shiny/ViewTabs";
import TableLegend, { keyFromBlocks, ViewIntro } from "./shiny/TableKey";
import layout from "@/data/layoutUi.json";
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
  useTabData,
  useTabQuery,
  useVersion,
  type DrawerSection,
} from "./shiny/TabKit";
type Payload = {
  rows: Row[];
  previous: Row[];
  meta: {
    version: string;
    showDelta: boolean;
    teams: { id: number; name: string }[];
    rounds: number[];
    phases: string[];
    seasons?: string[];
  };
};
const EMPTY: Row[] = [];
const LABELS: Record<TeamView, string> = {
  summary: "Summary",
  four_factors: "Four Factors",
  shot_profile: "Shot Profile",
  traditional: "Traditional",
};
export default function TeamRatingsClient({
  league,
  initial,
}: {
  league: League;
  initial?: { url: string; data: Payload | null };
}) {
  const euro = league !== "il",
    p = euro ? "euroteam" : "tr";
  const { query, set } = useTabQuery(league, TEAM_QUERY_DEFAULTS),
    view = (
      Object.keys(LABELS).includes(query.view) ? query.view : "summary"
    ) as TeamView;
  const url = teamRatingsPath(league, query),
    { data, loading, error, retry, draw, version } = useTabData<Payload>(
      url,
      initial,
    ),
    updated = useVersion(league, version, retry);
  const filters = useMemo(() => parseStats(query.stats), [query.stats]);
  const rows = useMemo(
    () =>
      teamTableRows(
        data?.rows ?? EMPTY,
        data?.previous ?? EMPTY,
        view,
        filters,
        data?.meta.showDelta ?? false,
        query.side === "defense",
      ),
    [data, view, filters, query.side],
  );
  const teams = teamChoices(league, query.season, data?.meta.teams),
    bounds = seasonBounds(league, query.season),
    frag = ui.teamRatings[euro ? "euro" : "il"];
  const views = euro
    ? (["summary", "four_factors"] as TeamView[])
    : (Object.keys(LABELS) as TeamView[]);
  const reset = () =>
    set({
      ...Object.fromEntries(
        Object.keys(queryDefaults(league, TEAM_QUERY_DEFAULTS)).map((k) => [
          k,
          null,
        ]),
      ),
      season: query.season,
      ...(euro ? { view: query.view } : {}),
    });
  const chips = contextChips({
    p,
    league,
    query,
    set,
    teams,
    datesAlways: !euro,
    teamKey: "teams",
    teamFocus: `${p}_teams`,
    datesFocus: `${p}_dates`,
  });
  if (query.clutch === "true")
    chips.push(
      <Chip
        key="clutch"
        label={`Clutch: ${query.clutch_margin} pts / ${query.clutch_minutes} min`}
        onClear={() => set({ clutch: null })}
      />,
    );
  const changeView = (label: string) =>
    set({
      view:
        Object.keys(LABELS).find((k) => LABELS[k as TeamView] === label) ??
        "summary",
      stats: null,
    });
  const viewKey = keyFromBlocks(frag.views[view] as Block[]);
  // The Israeli tab's date chip is always shown (Shiny); it is not a filter
  // until the dates move off the season's bounds.
  const activeCount =
    chips.length - (!euro && !query.start && !query.end ? 1 : 0);
  const drawerSections: DrawerSection[] = [
    {
      id: "dates",
      title: euro ? "Dates and teams" : "Dates",
      body: (
        <>
          <DateRangeInput
            id={`${p}_dates`}
            label={euro ? "Game Date Range" : "Date range"}
            start={query.start || bounds.start}
            end={query.end || bounds.end}
            min={bounds.start}
            max={bounds.end}
            onChange={(start, end) => set({ start, end })}
          />
          {euro && (
            <Selectize
              id={`${p}_teams`}
              label="Teams"
              choices={teams}
              multiple
              value={query.teams.split(",").filter(Boolean)}
              onChange={(v) => set({ teams: v.join(",") })}
              placeholder="All teams"
            />
          )}
        </>
      ),
    },
    {
      id: "clutch",
      title: "Clutch",
      body: <ClutchFilters p={p} query={query} set={set} />,
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
  return (
    <TabFrame
      league={league}
      tab="team_ratings"
      paneValue={euro ? "euro_team" : "team_ratings"}
      season={query.season}
      seasons={
        euro ? (data?.meta.seasons ?? ui.euroStaticSeasons) : ui.ilSeasons
      }
      onSeason={(season) =>
        set({
          season,
          start: null,
          end: null,
          teams: null,
          opponents: null,
          phase: null,
          min_gn: null,
          max_gn: null,
          last_n: null,
        })
      }
      views={views.map((v) => LABELS[v])}
      view={LABELS[view]}
      onView={changeView}
      updated={updated}
      filtersId={`${p}-filters`}
      layout="drawer"
      sidebar={null}
      drawer={{
        sections: drawerSections,
        activeCount,
        resettable: filters.length > 0,
        onClearAll: reset,
        resultLabel: `Show ${rows.length} teams`,
      }}
      main={
        <>
          <ChipsRow
            p={p}
            seasonChip={seasonChipLabel(league, query.season)}
            chips={chips}
            activeCount={activeCount}
            active={activeCount > 0 || filters.length > 0}
            onClearAll={reset}
            extra={
              !euro && (
                <StatFilters
                  p={p}
                  cols={ui.teamStatFilterCols[view]}
                  filters={filters}
                  onChange={(f) =>
                    set({ stats: f.length ? JSON.stringify(f) : null })
                  }
                />
              )
            }
            controls={
              // The Traditional view's own switches stay on the page.
              <div className="tr-trad-controls">
                {view === "traditional" && (
                  <>
                    <div className="d-flex align-items-center gap-2 mb-2">
                      <span>Team</span>
                      <div className="form-check form-switch mb-0">
                        <input
                          type="checkbox"
                          className="form-check-input"
                          role="switch"
                          aria-label="Opponent statistics"
                          checked={query.side === "defense"}
                          onChange={(e) =>
                            set({
                              side: e.target.checked ? "defense" : "offense",
                            })
                          }
                        />
                      </div>
                      <span>Opponent</span>
                    </div>
                    <Selectize
                      id="tr_trad_display_mode"
                      label="Traditional display mode"
                      choices={[
                        { value: "per_game", label: "Per Game" },
                        { value: "per75", label: "Per 75 Possessions" },
                        { value: "per40", label: "Per 40 Minutes" },
                      ]}
                      value={[query.display]}
                      onChange={(v) => set({ display: v[0] ?? "per_game" })}
                    />
                  </>
                )}
              </div>
            }
          />
          <ViewTabs
            views={views.map((v) => LABELS[v])}
            view={LABELS[view]}
            onView={changeView}
          />
          <div className="table-top">
            <ViewIntro
              blocks={frag.views[view] as Block[]}
              summary={
                layout.intro.teamRatings[
                  view as keyof typeof layout.intro.teamRatings
                ]
              }
            />
            <TableLegend
              league={league}
              shots={viewKey.shots}
              rank={viewKey.rank}
            />
          </div>
          <OutputArea
            id={euro ? "euroteam_table" : "tr_table"}
            loading={loading}
            error={error}
            retry={retry}
          >
            {data ? (
              <TeamRatingsTable
                key={JSON.stringify([
                  draw,
                  view,
                  query.side,
                  query.display,
                  query.stats,
                ])}
                rows={rows}
                view={view}
                league={league}
                side={query.side}
                display={query.display}
              />
            ) : (
              <p role="status">Loading Team Ratings…</p>
            )}
          </OutputArea>
        </>
      }
    />
  );
}
