"use client";
import { useMemo } from "react";
import type { League } from "@/rules/gameLogs";
import type { PlayerStatsPayload } from "@/server/playerStatsRepo";
import {
  PLAYER_MODES,
  playerDisplay,
  type PlayerMode,
} from "@/rules/playerStatsTable";
import {
  PLAYER_QUERY_DEFAULTS,
  playerStatsPath,
  queryDefaults,
} from "@/lib/tabQuery";
import ui from "@/data/shinyUi.json";
import PlayerStatsTable from "./shiny/PlayerStatsTable";
import Selectize from "./shiny/Selectize";
import DateRangeInput from "./shiny/DateRangeInput";
import ClutchFilters from "./shiny/ClutchFilters";
import { TableHint, type Block } from "./shiny/Blocks";
import {
  TabFrame,
  useTabQuery,
  useTabData,
  useVersion,
  teamChoices,
  seasonBounds,
  ViewRadios,
  GameFiltersAccordion,
  ViewContent,
  ChipsRow,
  Chip,
  contextChips,
  seasonChipLabel,
  StatFilters,
  parseStats,
  OutputArea,
  list,
} from "./shiny/TabKit";
export default function PlayerStatsClient({
  league,
  initial,
}: {
  league: League;
  initial?: { url: string; data: PlayerStatsPayload | null };
}) {
  const { query, set } = useTabQuery(league, PLAYER_QUERY_DEFAULTS),
    euro = league !== "il";
  const mode = PLAYER_MODES.includes(query.view as PlayerMode)
    ? (query.view as PlayerMode)
    : "Per Game";
  const { data, loading, error, retry, draw, version } =
    useTabData<PlayerStatsPayload>(playerStatsPath(league, query), initial);
  const updated = useVersion(league, version, retry),
    filters = useMemo(() => parseStats(query.stats), [query.stats]);
  const maxGp = Math.max(
    1,
    ...(data?.rows ?? []).map((r) => Number(r.gp) || 0),
  );
  const minGp = Math.min(
    maxGp,
    Math.max(1, Math.trunc(Number(query.min_gp) || 1)),
  );
  const result = useMemo(
    () =>
      playerDisplay(data?.rows ?? [], data?.identities ?? [], {
        mode,
        players: list(query.players),
        minGp,
        showIneligible: query.show_ineligible === "true",
        filters,
      }),
    [data, mode, query.players, minGp, query.show_ineligible, filters],
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
      sidebarTop={
        <ViewRadios
          name="ts_display_mode"
          label="Display mode"
          views={[...PLAYER_MODES]}
          view={mode}
          onView={onMode}
        />
      }
      sidebar={
        <>
          <button
            type="button"
            className="btn btn-default action-button"
            onClick={reset}
          >
            Reset Filters
          </button>
          <hr />
          <DateRangeInput
            id="ts_dates"
            label="Date range"
            start={query.start || bounds.start}
            end={query.end || bounds.end}
            min={bounds.start}
            max={bounds.end}
            onChange={(start, end) => set({ start, end })}
          />
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
          <div className="row">
            <div className="col-7">
              <label htmlFor="ts_min_gp_slider">Min GP</label>
              <input
                id="ts_min_gp_slider"
                aria-label="Min GP"
                className="w-100"
                type="range"
                min={1}
                max={maxGp}
                value={minGp}
                onChange={(e) => set({ min_gp: e.target.value })}
              />
            </div>
            <div className="col-5">
              <label htmlFor="ts_min_gp">Min GP (type)</label>
              <input
                className="form-control"
                id="ts_min_gp"
                type="number"
                min={1}
                max={maxGp}
                value={minGp}
                onChange={(e) =>
                  set({
                    min_gp: String(
                      Math.min(maxGp, Math.max(1, Number(e.target.value) || 1)),
                    ),
                  })
                }
              />
            </div>
          </div>
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
          <ClutchFilters p="ts" query={query} set={set} />
          <hr />
          <GameFiltersAccordion
            p="ts"
            league={league}
            query={query}
            set={set}
            teams={teams}
            rounds={data?.meta.rounds ?? []}
            phases={data?.meta.phases ?? []}
            oppRank
          />
        </>
      }
      main={
        <>
          <ViewContent
            blocks={(ui.playerStats.blocks as Block[]).filter(
              (b) => !euro || b.kind !== "example",
            )}
          />
          <ChipsRow
            p="ts"
            seasonChip={seasonChipLabel(league, query.season)}
            chips={chips}
            active={chips.length > 0 || filters.length > 0}
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
          />
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
          <TableHint hint={ui.playerStats.hint} />
          <OutputArea
            id="ts_table"
            loading={loading}
            error={error}
            retry={retry}
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
