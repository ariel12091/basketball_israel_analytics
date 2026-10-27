"use client";
import { useState } from "react";
import GameflowPanel from "./shiny/GameflowPanel";
// The Game Logs tab as Shiny draws it: ui_tab4_gamelogs.R (Israeli League) and
// ui_tab11_euro_gamelogs.R (EuroLeague, EuroCup), with build_filter_chips(),
// stat_filter_chips_ui() and setup_chip_clears(). The API computes the table
// (gameLogs()); the shared pieces live in shiny/TabKit.
import type { League, Row } from "@/rules/gameLogs";
import { gameLogsPath } from "@/lib/tabQuery";
import ui from "@/data/shinyUi.json";
import Selectize from "./shiny/Selectize";
import DateRangeInput from "./shiny/DateRangeInput";
import GameLogsTable from "./shiny/GameLogsTable";
import {
  ChipsRow,
  contextChips,
  FILTER_TIPS,
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
  useTabData,
  useTabQuery,
  useVersion,
  ViewContent,
  ViewRadios,
} from "./shiny/TabKit";
import { TableHint, type Block } from "./shiny/Blocks";

export { seasonBounds };
type Payload = {
  rows: Row[];
  meta: {
    version: string;
    view?: "summary" | "four_factors";
    teams: { id: number; name: string }[];
    rounds: number[];
    phases: string[];
    seasons?: string[];
    shot_averages: Record<string, Record<string, number>> | null;
  };
};
const VIEWS = ["Summary", "Four Factors"];
const EMPTY: Row[] = [];

export default function GameLogsClient({
  league,
  initial,
}: {
  league: League;
  initial?: { url: string; data: Payload | null };
}) {
  const euro = league !== "il";
  const [gameflow, setGameflow] = useState<Row | null>(null);
  const p = euro ? "eurogl" : "gl";
  const { query, set } = useTabQuery(league);
  const view = query.view === "four_factors" ? "four_factors" : "summary";
  const stats = euro ? [] : parseStats(query.stats);
  const bounds = seasonBounds(league, query.season);
  const url = gameLogsPath(league, query);
  const { data, loading, error, draw, retry, version } = useTabData<Payload>(
    url,
    initial,
  );
  const updated = useVersion(league, version, retry);

  const teams = teamChoices(league, query.season, data?.meta.teams);
  const teamKey = euro ? "teams" : "team";
  const picked = list(query[teamKey]);
  const seasons = euro
    ? (data?.meta.seasons ?? ui.euroStaticSeasons)
    : ui.ilSeasons;
  const reset = () =>
    set({
      view: null,
      team: null,
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
      min_gn: null,
      max_gn: null,
      last_n: null,
      stats: null,
    });
  const changeView = (v: string) =>
    set({ view: v === "Four Factors" ? "four_factors" : null, stats: null });
  const chips = contextChips({
    p,
    league,
    query,
    set,
    teams,
    datesAlways: !euro,
    teamKey,
    teamFocus: euro ? `${p}_teams` : `${p}_team`,
    datesFocus: `${p}_dates`,
  });
  const html = euro ? ui.euro : ui.il;
  const rows = data?.rows ?? EMPTY;

  const teamPicker = (
    <Selectize
      id={euro ? `${p}_teams` : `${p}_team`}
      label={euro ? "Teams" : "Team"}
      multiple={euro}
      placeholder={euro ? "All teams" : undefined}
      emptyLabel={euro ? undefined : "-- All teams --"}
      choices={teams}
      value={picked}
      onChange={(v) => set({ [teamKey]: v.join(",") })}
    />
  );
  const start = query.start || bounds.start,
    end = query.end || bounds.end;
  const dates = (
    <DateRangeInput
      id={`${p}_dates`}
      label={euro ? "Game Date Range" : "Date range"}
      start={start}
      end={end}
      min={bounds.start}
      max={bounds.end}
      onChange={(s, e) => {
        const full = s === bounds.start && e === bounds.end;
        set({ start: full ? null : s, end: full ? null : e });
      }}
    />
  );
  return (
    <TabFrame
      league={league}
      tab="game_logs"
      paneValue={euro ? "euro_game_logs" : "game_logs"}
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
          ...(euro ? {} : { team: null }),
        })
      }
      views={VIEWS}
      view={view === "four_factors" ? "Four Factors" : "Summary"}
      onView={changeView}
      updated={updated}
      filtersId={`${p}-filters`}
      sidebarTop={
        <ViewRadios
          name={`${p}_view_mode`}
          label="View:"
          views={VIEWS}
          view={view === "four_factors" ? "Four Factors" : "Summary"}
          tips={{
            Summary: FILTER_TIPS.view_summary,
            "Four Factors": FILTER_TIPS.view_ff,
          }}
          onView={changeView}
        />
      }
      sidebar={
        <>
          <button
            type="button"
            className="btn btn-default action-button"
            onClick={reset}
          >
            {euro ? "Reset to defaults" : "Reset Filters"}
          </button>
          <hr />
          {euro ? (
            <>
              {dates}
              {teamPicker}
            </>
          ) : (
            <>
              {teamPicker}
              {dates}
            </>
          )}
          <StarterRows p={p} query={query} set={set} />
          <hr />
          <GameFiltersAccordion
            p={p}
            league={league}
            query={query}
            set={set}
            teams={teams}
            rounds={data?.meta.rounds ?? []}
            phases={data?.meta.phases ?? []}
            oppRank={false}
          />
        </>
      }
      main={
        <>
          <div style={view === "summary" ? undefined : { display: "none" }}>
            <ViewContent blocks={html.views.summary as Block[]} />
          </div>
          <div
            style={view === "four_factors" ? undefined : { display: "none" }}
          >
            <ViewContent blocks={html.views.four_factors as Block[]} />
          </div>
          <ChipsRow
            p={p}
            seasonChip={seasonChipLabel(league, query.season)}
            chips={chips}
            extra={
              !euro && (
                <StatFilters
                  p={p}
                  cols={ui.statFilterCols[view]}
                  filters={stats}
                  onChange={(f) =>
                    set({ stats: f.length ? JSON.stringify(f) : null })
                  }
                />
              )
            }
            active={chips.length > 0 || stats.length > 0}
            onClearAll={reset}
          />
          <TableHint hint={html.hint} />
          <OutputArea
            id={`${p}_table`}
            loading={loading}
            error={error}
            retry={retry}
          >
            {!data ? null : rows.length ? (
              <GameLogsTable
                key={draw}
                league={league}
                view={data.meta.view ?? view}
                rows={rows}
                shotAverages={data.meta.shot_averages}
                onGameflow={setGameflow}
              />
            ) : euro ? (
              // empty_game_log() in server_tab11_euro_gamelogs.R
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
                      <td>No games match the current filters</td>
                    </tr>
                  </tbody>
                </table>
              </div>
            ) : null}
          </OutputArea>
          {gameflow && (
            <GameflowPanel
              league={league}
              game={Number(gameflow.game_id)}
              team={Number(gameflow.team_id)}
              onClose={() => setGameflow(null)}
            />
          )}
        </>
      }
    />
  );
}
