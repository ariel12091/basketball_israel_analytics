"use client";
import { useMemo } from "react";
import type { League } from "@/rules/gameLogs";
import type { PlayerStatsPayload } from "@/server/playerStatsRepo";
import {
  PLAYER_COLUMN_VIEWS,
  PLAYER_MODES,
  parsePlayerColumnView,
  playerColumnViewParam,
  playerDisplay,
  type PlayerColumnView,
  type PlayerMode,
} from "@/rules/playerStatsTable";
import {
  PLAYER_QUERY_DEFAULTS,
  playerStatsPath,
  queryDefaults,
} from "@/lib/tabQuery";
import ui from "@/data/shinyUi.json";
import { playerStatsPageOptions } from "@/lib/pageState/playerStats";
import PlayerStatsTable from "./shiny/PlayerStatsTable";
import Selectize from "./shiny/Selectize";
import DateRangeInput from "./shiny/DateRangeInput";
import ClutchFilters from "./shiny/ClutchFilters";
import { type Block } from "./shiny/Blocks";
import ViewTabs from "./shiny/ViewTabs";
import PillSwitch from "./shiny/PillSwitch";
import PossStepper from "./shiny/PossStepper";
import TableLegend, { keyFromBlocks, ViewIntro } from "./shiny/TableKey";
import { ToolsSlot } from "./shiny/toolsSlot";
import layout from "@/data/layoutUi.json";
import {
  TabFrame,
  useTabQuery,
  useTabData,
  useVersion,
  teamChoices,
  seasonBounds,
  defaultDates,
  GameFiltersAccordion,
  type DrawerSection,
  ChipsRow,
  Chip,
  contextChips,
  seasonChipLabel,
  StatFilters,
  OutputArea,
  list,
} from "./shiny/TabKit";
const MODE_LABELS: Record<PlayerMode, string> = {
  Totals: "Totals",
  "Per Game": "Per game",
  "Per 60 Possessions": "Per 60 poss",
  "Per 30 Minutes": "Per 30 min",
};
export default function PlayerStatsClient({
  league,
  initial,
}: {
  league: League;
  initial?: { url: string; data: PlayerStatsPayload | null };
}) {
  const { query, set } = useTabQuery(league, PLAYER_QUERY_DEFAULTS),
    euro = league !== "il";
  const { data, loading, error, retry, draw, version } =
    useTabData<PlayerStatsPayload>(playerStatsPath(league, query), initial);
  const updated = useVersion(league, version, retry);
  // The same page state the API's shape=table and the chat read.
  const options = useMemo(
    () => playerStatsPageOptions(query, data?.rows ?? []),
    [query, data],
  );
  const { mode, minGp, maxGp, filters } = options;
  const result = useMemo(
    () => playerDisplay(data?.rows ?? [], data?.identities ?? [], options),
    [data, options],
  );
  const teams = teamChoices(league, query.season, data?.meta.teams),
    bounds = seasonBounds(league, query.season);
  const choices = (data?.players ?? [])
    .filter(
      (r) => !query.teams || list(query.teams).includes(String(r.team_id)),
    )
    .map((r) => {
      const id = `${r.team_id}:${r.player_id}`,
        identity = data?.identities.find(
          (i) => `${i.team_id}:${i.player_id}` === id,
        );
      return {
        value: id,
        label: `${identity?.display_name || r.player_name} (${teams.find((t) => t.value === String(r.team_id))?.label ?? r.team_name ?? r.team_id})`,
      };
    });
  const reset = () =>
    set({
      ...Object.fromEntries(
        Object.keys(queryDefaults(league, PLAYER_QUERY_DEFAULTS)).map((k) => [
          k,
          null,
        ]),
      ),
      season: query.season,
      // Clearing filters keeps the column view (user 2026-11-17); the mode
      // still resets, as Shiny's Reset Filters did.
      cols: query.cols,
    });
  const chips = contextChips({
    p: "ts",
    league,
    query,
    set,
    teams,
    datesAlways: true,
    teamKey: "teams",
    teamFocus: "ts_teams",
    datesFocus: "ts_dates",
  });
  if (query.players)
    chips.push(
      <Chip
        key="players"
        label={
          list(query.players).length === 1
            ? `Player: ${choices.find((c) => c.value === query.players)?.label ?? query.players}`
            : `${list(query.players).length} players`
        }
        onClear={() => set({ players: null })}
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
  const onMode = (view: string) => set({ view });
  const columnView = parsePlayerColumnView(query.cols);
  const introBlocks = (ui.playerStats.blocks as Block[]).filter(
    (b) => !euro || b.kind !== "example",
  );
  const viewKey = keyFromBlocks(introBlocks);
  // The date chip is always shown (Shiny); it is not a filter until the
  // dates move off the season's bounds.
  const activeCount = chips.length - (!query.start && !query.end ? 1 : 0);
  const drawerSections: DrawerSection[] = [
    {
      id: "dates",
      title: "Dates",
      body: (
        <>
          <DateRangeInput
            id="ts_dates"
            label="Date range"
            start={query.start || defaultDates(league, query.season).start}
            end={query.end || defaultDates(league, query.season).end}
            min={bounds.start}
            max={bounds.end}
            onChange={(start, end) => set({ start, end })}
          />
        </>
      ),
    },
    {
      id: "clutch",
      title: "Clutch",
      body: <ClutchFilters p="ts" query={query} set={set} />,
    },
    {
      id: "game",
      title: "Game filters",
      body: (
        <GameFiltersAccordion
          p="ts"
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
      tab="traditional_stats"
      paneValue="traditional_stats"
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
          players: null,
          opponents: null,
          phase: null,
          min_gn: null,
          max_gn: null,
          last_n: null,
        })
      }
      views={[...PLAYER_MODES]}
      view={mode}
      onView={onMode}
      updated={updated}
      filtersId="ts-filters"
      layout="drawer"
      sidebar={null}
      drawer={{
        sections: drawerSections,
        activeCount,
        resettable: filters.length > 0,
        onClearAll: reset,
      }}
      main={
        <>
          {/* Teams and players sit above the table, always in reach, as on
              Lineups (user 2026-10-28). */}
          <div className="lineup-bar ps-bar">
            <Selectize
              id="ts_teams"
              label="Teams"
              multiple
              choices={teams}
              value={list(query.teams)}
              onChange={(v) =>
                set({
                  teams: v.join(","),
                  players: choices
                    .filter(
                      (c) =>
                        list(query.players).includes(c.value) &&
                        (!v.length || v.includes(c.value.split(":")[0])),
                    )
                    .map((c) => c.value)
                    .join(","),
                })
              }
              placeholder="All teams"
            />
            <Selectize
              id="ts_players"
              label="Players"
              multiple
              choices={choices}
              value={list(query.players)}
              onChange={(v) => set({ players: v.join(",") })}
              placeholder="All players"
            />
          </div>
          {/* Column views take the tabs; the mode is a pill switch beside
              them (design 2026-11-17). */}
          <div className="ps-views-row">
            <ViewTabs
              views={[...PLAYER_COLUMN_VIEWS]}
              view={columnView}
              onView={(v) =>
                set({ cols: playerColumnViewParam(v as PlayerColumnView) })
              }
            />
            <PillSwitch
              label="Mode"
              options={PLAYER_MODES.map((m) => ({
                value: m,
                label: MODE_LABELS[m],
                title: m,
              }))}
              value={mode}
              onChange={onMode}
            />
          </div>
          <ChipsRow
            p="ts"
            seasonChip={seasonChipLabel(league, query.season)}
            chips={chips}
            activeCount={activeCount}
            active={activeCount > 0 || filters.length > 0}
            onClearAll={reset}
            extra={
              <StatFilters
                p="ts"
                labels={ui.playerStatFilterCols}
                cols={ui.playerStatFilterCols.filter(
                  (c) =>
                    (!euro || c.col !== "dfl") &&
                    (mode !== "Totals" || c.col !== "total_poss"),
                )}
                filters={filters}
                onChange={(f) =>
                  set({ stats: f.length ? JSON.stringify(f) : null })
                }
              />
            }
            controls={
              <>
                <PossStepper
                  id="ts_min_gp"
                  label="Min GP"
                  name="Min GP"
                  min={1}
                  max={maxGp}
                  step={1}
                  value={minGp}
                  onChange={(v) => set({ min_gp: String(v) })}
                />
                <label className="checkbox">
                  <input
                    type="checkbox"
                    checked={query.show_ineligible === "true"}
                    onChange={(e) =>
                      set({ show_ineligible: String(e.target.checked) })
                    }
                  />{" "}
                  Show non-eligible players in rate modes
                </label>
              </>
            }
          />
          <div className="table-top">
            <ViewIntro
              blocks={introBlocks}
              summary={layout.intro.playerStats}
            />
            <TableLegend shots={viewKey.shots} rank={viewKey.rank} />
            <ToolsSlot />
          </div>
          {result.ineligible > 0 && (
            <div
              className={`alert ${query.show_ineligible === "true" ? "alert-info" : "alert-warning"} py-2 mb-2`}
              role="status"
            >
              {query.show_ineligible === "true"
                ? `${result.ineligible} non-eligible players shown in gray in ${mode}`
                : `${result.removed} players hidden in ${mode}`}{" "}
              (below {Math.trunc(result.threshold).toLocaleString("en-US")}{" "}
              total possessions).
            </div>
          )}
          <OutputArea
            id="ts_table"
            loading={loading}
            error={error}
            retry={retry}
            onClear={reset}
          >
            {data ? (
              <PlayerStatsTable
                key={JSON.stringify([
                  draw,
                  mode,
                  query.players,
                  minGp,
                  query.stats,
                  query.show_ineligible,
                ])}
                rows={result.rows}
                league={league}
                mode={mode}
                columnView={columnView}
              />
            ) : (
              <p role="status">Loading Player Stats…</p>
            )}
          </OutputArea>
        </>
      }
    />
  );
}
