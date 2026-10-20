"use client";
import Link from "next/link";
import { useEffect, useMemo, useRef, useState } from "react";
import { parseAsString, useQueryStates } from "nuqs";
import {
  flexRender,
  getCoreRowModel,
  getSortedRowModel,
  useReactTable,
  type ColumnDef,
  type SortingState,
} from "@tanstack/react-table";
import { toCsv } from "@/server/csv";
import type { League, Row, StatFilter } from "@/rules/gameLogs";
import { HEAT_REVERSE } from "@/rules/gameLogs";
import catalogues from "@/data/catalogues.json";
import TeamPicker from "./TeamPicker";

type Payload = {
  rows: Row[];
  meta: {
    version: string;
    teams: { id: number; name: string }[];
    rounds: number[];
    phases: string[];
    shot_averages: Record<string, Record<string, number>> | null;
  };
};
const leagues: { id: League; name: string }[] = [
  { id: "il", name: "Israeli League" },
  { id: "euroleague", name: "EuroLeague" },
  { id: "eurocup", name: "EuroCup" },
];
const EMPTY_ROWS: Row[] = [];
const oneDecimal = new Set([
  "minutes",
  "off_ppp",
  "def_ppp",
  "net_rtg",
  "off_efg_pct",
  "off_oreb_pct",
  "off_tov_pct",
  "off_ftr_pct",
  "def_efg_pct",
  "def_oreb_pct",
  "def_tov_pct",
  "def_ftr_pct",
]);
const summaryColumns = [
  ["minutes", "Min"],
  ["off_ppp", "Off PPP"],
  ["def_ppp", "Def PPP"],
  ["net_rtg", "Net"],
];
const ffColumns = [
  ["minutes", "Min"],
  ["net_rtg", "Net"],
  ["off_ppp", "Off PPP"],
  ["off_efg_pct", "Off eFG%"],
  ["off_oreb_pct", "Off OREB%"],
  ["off_tov_pct", "Off TOV%"],
  ["off_ftr_pct", "Off FTR"],
  ["def_ppp", "Def PPP"],
  ["def_efg_pct", "Def eFG%"],
  ["def_oreb_pct", "Def OREB%"],
  ["def_tov_pct", "Def TOV%"],
  ["def_ftr_pct", "Def FTR"],
];
const statLabels = [
  ...summaryColumns,
  ...ffColumns,
  ["off_poss", "Off Poss"],
  ["def_poss", "Def Poss"],
  ["Off Shot", "Off Shot"],
  ["Def Shot", "Def Shot"],
].filter((x, i, a) => a.findIndex((y) => y[0] === x[0]) === i);
function formatted(value: unknown, id: string) {
  return value == null
    ? "—"
    : typeof value === "number"
      ? value.toLocaleString("en-US", {
          minimumFractionDigits: oneDecimal.has(id) ? 1 : 0,
          maximumFractionDigits: oneDecimal.has(id) ? 1 : 0,
        })
      : String(value);
}
function shotText(row: Row, side: string) {
  return [2, 3]
    .map(
      (fg) =>
        `${fg}PT ${formatted(row[`${side}_fg${fg}_freq`], "")}% · ${row[`${side}_fg${fg}m`] ?? 0}/${row[`${side}_fg${fg}a`] ?? 0} (${formatted(row[`${side}_fg${fg}_pct`], "")}%)`,
    )
    .join("; ");
}
function parseStats(value: string): StatFilter[] {
  try {
    const data = JSON.parse(value || "[]");
    return Array.isArray(data)
      ? data.filter(
          (f) =>
            f &&
            typeof f.col === "string" &&
            ["ge", "le"].includes(f.op) &&
            typeof f.value === "number" &&
            Number.isFinite(f.value),
        )
      : [];
  } catch {
    return [];
  }
}

export default function GameLogsClient({ league }: { league: League }) {
  const now = new Date(),
    current =
      now.getFullYear() +
      Number(now.getMonth() >= 8) -
      (league === "il" ? 0 : 1);
  const [query, setQuery] = useQueryStates(
    {
      season: parseAsString.withDefault(String(current)),
      view: parseAsString.withDefault("summary"),
      team: parseAsString.withDefault(""),
      teams: parseAsString.withDefault(""),
      start: parseAsString.withDefault(""),
      end: parseAsString.withDefault(""),
      own_starters: parseAsString.withDefault(""),
      opp_starters: parseAsString.withDefault(""),
      game_type: parseAsString.withDefault(""),
      phase: parseAsString.withDefault(""),
      opponents: parseAsString.withDefault(""),
      home_away: parseAsString.withDefault(""),
      outcome: parseAsString.withDefault(""),
      min_gn: parseAsString.withDefault(""),
      max_gn: parseAsString.withDefault(""),
      last_n: parseAsString.withDefault(""),
      stats: parseAsString.withDefault(""),
    },
    { history: "replace", shallow: true },
  );
  const [data, setData] = useState<Payload | null>(null),
    [loading, setLoading] = useState(true),
    [error, setError] = useState(""),
    [filtersOpen, setFiltersOpen] = useState(false),
    [revision, setRevision] = useState(0);
  const [sorting, setSorting] = useState<SortingState>([
    { id: "game_date", desc: true },
    { id: league === "il" ? "gn" : "round_number", desc: true },
  ]);
  const [statCol, setStatCol] = useState("off_ppp"),
    [statOp, setStatOp] = useState<"ge" | "le">("ge"),
    [statValue, setStatValue] = useState("");
  const currentVersion = useRef("");
  const params = useMemo(() => {
    const p = new URLSearchParams();
    for (const [key, value] of Object.entries(query))
      if (value) p.set(key, value);
    return p.toString();
  }, [query]);
  useEffect(() => {
    const controller = new AbortController();
    let active = true;
    const timer = setTimeout(() => {
      setLoading(true);
      setError("");
      void fetch(`/api/v1/${league}/game-logs?${params}`, {
        signal: controller.signal,
      })
        .then(async (response) => {
          const answer = await response.json();
          if (!response.ok)
            throw new Error(answer.error ?? "Unable to load games");
          if (active) {
            setData(answer);
            currentVersion.current = answer.meta.version;
          }
        })
        .catch((e) => {
          if (active && e.name !== "AbortError") setError(e.message);
        })
        .finally(() => {
          if (active) setLoading(false);
        });
    }, 120);
    return () => {
      active = false;
      clearTimeout(timer);
      controller.abort();
    };
  }, [league, params, revision]);
  useEffect(() => {
    const check = () => {
      void fetch("/api/v1/version")
        .then((r) => (r.ok ? r.json() : null))
        .then((value) => {
          const version = value?.leagues?.find(
            (x: { league: string }) => x.league === league,
          )?.version;
          if (
            version &&
            currentVersion.current &&
            version !== currentVersion.current
          ) {
            currentVersion.current = version;
            setRevision((r) => r + 1);
          }
        })
        .catch(() => {});
    };
    const timer = setInterval(check, 60000);
    window.addEventListener("focus", check);
    return () => {
      clearInterval(timer);
      window.removeEventListener("focus", check);
    };
  }, [league]);
  const euro = league !== "il",
    view = query.view === "four_factors" ? "four_factors" : "summary",
    stats = parseStats(query.stats);
  const columnPairs = useMemo(
    () => [
      ...[
        euro ? ["round_number", "Rd"] : ["gn", "GN"],
        euro ? ["phase_label", "Phase"] : ["game_type_label", "Game Type"],
        ["game_date", "Date"],
        ["team_name", "Team"],
        ["opp_team_name", "Opponent"],
        ["result", "W/L"],
        [euro ? "score" : "score_display", "Score"],
      ],
      ...(view === "four_factors" ? ffColumns : summaryColumns),
      ...(!euro && view === "summary"
        ? [
            ["Off Shot", "Off Shot"],
            ["Def Shot", "Def Shot"],
          ]
        : []),
      ["off_poss", "Off Poss"],
      ["def_poss", "Def Poss"],
    ],
    [euro, view],
  );
  const columns = useMemo<ColumnDef<Row>[]>(
    () =>
      columnPairs.map(([id, label]) => ({
        id,
        header: label,
        accessorFn: (row) => row[id] ?? undefined,
        sortUndefined: "last",
        sortDescFirst: !HEAT_REVERSE[id],
        cell: (info) => {
          const row = info.row.original;
          if (id === "Off Shot" || id === "Def Shot") {
            const side = id === "Off Shot" ? "off" : "def";
            return (
              <div className="shots">
                {[2, 3].map((fg) => {
                  const freq = Number(row[`${side}_fg${fg}_freq`] ?? 0),
                    pct = row[`${side}_fg${fg}_pct`],
                    avg = data?.meta.shot_averages?.[side]?.[`fg${fg}`];
                  return (
                    <div key={fg} className="shot-line">
                      <span>{fg}PT</span>
                      <span
                        className={`frequency fg${fg}`}
                        style={{ width: `${Math.max(freq, 5)}%` }}
                        title={`${freq}% of attempts`}
                      />
                      <span
                        title={`${row[`${side}_fg${fg}m`] ?? 0} / ${row[`${side}_fg${fg}a`] ?? 0} made`}
                        className={
                          pct != null && avg != null
                            ? Number(pct) >= avg
                              ? "positive"
                              : "negative"
                            : ""
                        }
                      >
                        {pct == null ? "—" : `${formatted(pct, "")}%`}
                      </span>
                    </div>
                  );
                })}
              </div>
            );
          }
          const percentile = row["pr_" + id.replace(/[^A-Za-z0-9]+/g, "_")];
          let style = {};
          if (typeof percentile === "number") {
            const good = HEAT_REVERSE[id] ? 1 - percentile : percentile;
            style = {
              backgroundColor: `hsl(${Math.round(good * 120)} 55% 89%)`,
            };
          }
          return (
            <span
              className={typeof info.getValue() === "number" ? "metric" : ""}
              style={style}
            >
              {formatted(info.getValue(), id)}
            </span>
          );
        },
      })),
    [columnPairs, data?.meta.shot_averages],
  );
  const rows = data?.rows ?? EMPTY_ROWS;
  const table = useReactTable({
    data: rows,
    columns,
    state: { sorting },
    onSortingChange: setSorting,
    getCoreRowModel: getCoreRowModel(),
    getSortedRowModel: getSortedRowModel(),
  });
  const update = (key: keyof typeof query, value: string) => {
    void setQuery({
      [key]: value,
      ...(key === "last_n"
        ? { min_gn: "", max_gn: "" }
        : key === "min_gn" || key === "max_gn"
          ? { last_n: "" }
          : {}),
    });
  };
  const reset = () => {
    const blank = Object.fromEntries(Object.keys(query).map((k) => [k, null]));
    void setQuery({ ...blank, season: query.season, view: query.view });
  };
  const selections = Object.entries(query).filter(
    ([k, v]) => v && !["season", "view", "stats"].includes(k),
  );
  const multi = (key: "teams" | "opponents", label: string) => (
    <TeamPicker
      label={label}
      teams={data?.meta.teams ?? []}
      values={query[key].split(",").filter(Boolean)}
      multiple
      onChange={(values) => update(key, values.join(","))}
    />
  );
  const chipValue = (key: string, value: string) => {
    if (["team", "teams", "opponents"].includes(key))
      return value
        .split(",")
        .map(
          (id) =>
            data?.meta.teams.find((team) => String(team.id) === id)?.name ??
            (loading ? "Loading team…" : "Unknown team"),
        )
        .join(", ");
    if (key === "game_type")
      return value
        .split(",")
        .map((id) => (catalogues.gameTypes as Record<string, string>)[id] ?? id)
        .join(", ");
    if (key === "phase")
      return value
        .split(",")
        .map((id) => (catalogues.phases as Record<string, string>)[id] ?? id)
        .join(", ");
    return value;
  };
  const starter = (key: "own_starters" | "opp_starters", label: string) => {
    const [mode = "", value = ""] = query[key].split(":");
    return (
      <fieldset>
        <legend>{label}</legend>
        <div className="pair">
          <select
            aria-label={`${label} mode`}
            value={mode}
            onChange={(e) =>
              update(
                key,
                e.target.value ? `${e.target.value}:${value || "3"}` : "",
              )
            }
          >
            <option value="">All</option>
            <option value="gte">At least (≥)</option>
            <option value="lte">At most (≤)</option>
          </select>
          <select
            aria-label={`${label} value`}
            disabled={!mode}
            value={value || "3"}
            onChange={(e) => update(key, `${mode}:${e.target.value}`)}
          >
            {[0, 1, 2, 3, 4, 5].map((n) => (
              <option key={n}>{n}</option>
            ))}
          </select>
        </div>
      </fieldset>
    );
  };
  const exportCsv = () => {
    const ordered = table.getRowModel().rows.map((r) => ({
      ...r.original,
      ...(!euro && view === "summary"
        ? {
            "Off Shot": shotText(r.original, "off"),
            "Def Shot": shotText(r.original, "def"),
          }
        : {}),
    }));
    const ids = columnPairs.map(([id]) => id);
    const csv = toCsv(ordered, ids);
    const url = URL.createObjectURL(
      new Blob(["\uFEFF" + csv], { type: "text/csv;charset=utf-8" }),
    );
    const a = document.createElement("a");
    a.href = url;
    a.download = `${league}_game_logs_${view}_${new Date().toISOString().replace(/[:.]/g, "-")}.csv`;
    a.click();
    URL.revokeObjectURL(url);
  };
  return (
    <>
      <header className="site-header">
        <Link href="/il/game-logs" className="brand">
          IBPL <span>Analytics</span>
        </Link>
        <nav aria-label="League">
          {leagues.map((item) => (
            <Link
              key={item.id}
              className={league === item.id ? "active" : ""}
              href={`/${item.id}/game-logs`}
            >
              {item.name}
            </Link>
          ))}
        </nav>
        <div className="updated">
          Last updated
          <br />
          <time>{data?.meta.version ?? "Loading…"}</time>
        </div>
      </header>
      <main className="dashboard">
        <div className="page-title">
          <div>
            <p className="eyebrow">
              {leagues.find((l) => l.id === league)?.name}
            </p>
            <h1>Game Logs</h1>
          </div>
          <label className="season-picker">
            Season
            <select
              aria-label="Season"
              value={query.season}
              onChange={(e) => update("season", e.target.value)}
            >
              {Array.from({ length: 6 }, (_, i) => current - i).map((year) => (
                <option key={year} value={year}>
                  {euro
                    ? `${year}–${String(year + 1).slice(2)}`
                    : `${year - 1}–${String(year).slice(2)}`}
                </option>
              ))}
            </select>
          </label>
        </div>
        <div className="workspace">
          <aside className="sidebar">
            <button
              className="filter-toggle"
              aria-expanded={filtersOpen}
              onClick={() => setFiltersOpen(!filtersOpen)}
            >
              {filtersOpen ? "Hide Filters" : "Show Filters"}
            </button>
            <div className={`filters ${filtersOpen ? "open" : ""}`}>
              <div className="filter-heading">
                <h2>Filters</h2>
                <button onClick={reset} className="text-button">
                  Reset
                </button>
              </div>
              <fieldset>
                <legend>View</legend>
                <div className="view-switch">
                  {[
                    ["summary", "Summary"],
                    ["four_factors", "Four Factors"],
                  ].map(([value, label]) => (
                    <button
                      key={value}
                      className={view === value ? "selected" : ""}
                      onClick={() => {
                        void setQuery({ view: value, stats: "" });
                      }}
                    >
                      {label}
                    </button>
                  ))}
                </div>
              </fieldset>
              {euro ? (
                multi("teams", "Teams")
              ) : (
                <TeamPicker
                  label="Team"
                  teams={data?.meta.teams ?? []}
                  values={query.team ? [query.team] : []}
                  onChange={(values) => update("team", values[0] ?? "")}
                />
              )}
              <fieldset>
                <legend>Date range</legend>
                <div className="dates">
                  <label>
                    From
                    <input
                      aria-label="From date"
                      type="date"
                      value={query.start}
                      onChange={(e) => update("start", e.target.value)}
                    />
                  </label>
                  <label>
                    To
                    <input
                      aria-label="To date"
                      type="date"
                      value={query.end}
                      onChange={(e) => update("end", e.target.value)}
                    />
                  </label>
                </div>
              </fieldset>
              {starter("own_starters", "Own lineup starters")}
              {starter("opp_starters", "Opponent lineup starters")}
              <details open>
                <summary>Game context</summary>
                {euro ? (
                  <label>
                    Phase
                    <select
                      value={query.phase}
                      onChange={(e) => update("phase", e.target.value)}
                    >
                      <option value="">All phases</option>
                      {data?.meta.phases.map((phase) => (
                        <option key={phase} value={phase}>
                          {(catalogues.phases as Record<string, string>)[
                            phase
                          ] ?? phase}
                        </option>
                      ))}
                    </select>
                  </label>
                ) : (
                  <label>
                    Game type
                    <select
                      value={query.game_type}
                      onChange={(e) => update("game_type", e.target.value)}
                    >
                      {catalogues.gameTypeChoices.map((item) => (
                        <option key={item.value} value={item.value}>
                          {item.label}
                        </option>
                      ))}
                    </select>
                  </label>
                )}
                {multi("opponents", "Opponents")}
                <div className="pair">
                  <label>
                    Home / Away
                    <select
                      value={query.home_away}
                      onChange={(e) => update("home_away", e.target.value)}
                    >
                      <option value="">All</option>
                      <option value="home">Home</option>
                      <option value="away">Away</option>
                    </select>
                  </label>
                  <label>
                    Outcome
                    <select
                      value={query.outcome}
                      onChange={(e) => update("outcome", e.target.value)}
                    >
                      <option value="">All</option>
                      <option value="win">Win</option>
                      <option value="loss">Loss</option>
                    </select>
                  </label>
                </div>
                <div className="pair">
                  {["min_gn", "max_gn"].map((key, i) => (
                    <label key={key}>
                      {i ? "To" : "From"} {euro ? "Round" : "GN"}
                      <select
                        value={query[key as "min_gn"]}
                        onChange={(e) =>
                          update(key as "min_gn", e.target.value)
                        }
                      >
                        <option value="">All</option>
                        {data?.meta.rounds.map((round) => (
                          <option key={round}>{round}</option>
                        ))}
                      </select>
                    </label>
                  ))}
                </div>
                <label>
                  Last N games
                  <select
                    value={query.last_n}
                    onChange={(e) => update("last_n", e.target.value)}
                  >
                    <option value="">All games</option>
                    {Array.from({ length: 40 }, (_, i) => i + 1).map((n) => (
                      <option key={n}>{n}</option>
                    ))}
                  </select>
                </label>
              </details>
            </div>
          </aside>
          <section className="results" aria-label="Game results">
            <div className="explainer">
              <h2>
                {view === "summary"
                  ? "How did each game look?"
                  : "What drove each result?"}
              </h2>
              <p>
                {view === "summary"
                  ? "Each row is one team’s perspective on a game. Compare points per 100 possessions to see beyond the final score."
                  : "Compare shooting, rebounding, turnovers and free throws on both sides. Percentile colours use the whole season, so a game keeps its colour when you narrow the filters."}
              </p>
            </div>
            {!euro && (
              <form
                className="stat-form"
                onSubmit={(e) => {
                  e.preventDefault();
                  const value = Number(statValue);
                  if (statValue !== "" && Number.isFinite(value)) {
                    void setQuery({
                      stats: JSON.stringify([
                        ...stats,
                        { col: statCol, op: statOp, value },
                      ]),
                    });
                    setStatValue("");
                  }
                }}
              >
                <label className="sr-only" htmlFor="stat-column">
                  Stat
                </label>
                <select
                  id="stat-column"
                  value={statCol}
                  onChange={(e) => setStatCol(e.target.value)}
                >
                  {statLabels
                    .filter(([id]) => columnPairs.some(([col]) => col === id))
                    .map(([id, label]) => (
                      <option key={id} value={id}>
                        {label}
                      </option>
                    ))}
                </select>
                <select
                  aria-label="Stat comparison"
                  value={statOp}
                  onChange={(e) => setStatOp(e.target.value as "ge" | "le")}
                >
                  <option value="ge">≥</option>
                  <option value="le">≤</option>
                </select>
                <input
                  aria-label="Stat value"
                  type="number"
                  step="any"
                  placeholder="Value"
                  value={statValue}
                  onChange={(e) => setStatValue(e.target.value)}
                />
                <button type="submit">Add stat filter</button>
              </form>
            )}
            <div className="chips" aria-label="Active filters">
              {selections.map(([key, value]) => (
                <button
                  key={key}
                  onClick={() => update(key as keyof typeof query, "")}
                  aria-label={`Clear ${key}`}
                >
                  {key.replace(/_/g, " ")}: {chipValue(key, value)}
                  <span>×</span>
                </button>
              ))}
              {stats.map((stat, i) => (
                <button
                  key={i}
                  onClick={() => {
                    void setQuery({
                      stats: JSON.stringify(stats.filter((_, j) => j !== i)),
                    });
                  }}
                >
                  {stat.col} {stat.op === "ge" ? "≥" : "≤"} {stat.value}
                  <span>×</span>
                </button>
              ))}
            </div>
            <div className="table-toolbar">
              <span role="status">
                {loading
                  ? "Loading games…"
                  : `${rows.length.toLocaleString()} team-games`}
              </span>
              <button
                onClick={exportCsv}
                disabled={loading || Boolean(error) || !rows.length}
              >
                Download CSV ↓
              </button>
            </div>
            {error ? (
              <div role="alert" className="error">
                <p>{error}</p>
                <button onClick={() => setRevision((r) => r + 1)}>Retry</button>
              </div>
            ) : (
              <div className="table-scroll" aria-busy={loading}>
                <table aria-label="Game logs">
                  {view === "four_factors" && (
                    <>
                      <colgroup
                        span={columnPairs.findIndex(([id]) => id === "off_ppp")}
                      />
                      <colgroup span={5} />
                      <colgroup span={5} />
                      <colgroup span={2} />
                    </>
                  )}
                  <thead>
                    {view === "four_factors" && (
                      <tr className="column-groups">
                        <th
                          scope="colgroup"
                          colSpan={columnPairs.findIndex(
                            ([id]) => id === "off_ppp",
                          )}
                          aria-label="Game context"
                        />
                        <th scope="colgroup" colSpan={5}>
                          Offense
                        </th>
                        <th scope="colgroup" colSpan={5}>
                          Defense
                        </th>
                        <th scope="colgroup" colSpan={2}>
                          Usage
                        </th>
                      </tr>
                    )}
                    {table.getHeaderGroups().map((group) => (
                      <tr key={group.id}>
                        {group.headers.map((header) => (
                          <th key={header.id} scope="col">
                            <button
                              aria-label={`${header.column.columnDef.header} column`}
                              onClick={header.column.getToggleSortingHandler()}
                              title={`Sort by ${header.column.columnDef.header}`}
                            >
                              {view === "four_factors" &&
                              /^(off|def)_/.test(header.column.id) &&
                              !header.column.id.endsWith("_poss")
                                ? String(
                                    header.column.columnDef.header,
                                  ).replace(/^(Off|Def) /, "")
                                : flexRender(
                                    header.column.columnDef.header,
                                    header.getContext(),
                                  )}
                              <span>
                                {header.column.getIsSorted() === "desc"
                                  ? " ↓"
                                  : header.column.getIsSorted() === "asc"
                                    ? " ↑"
                                    : " ↕"}
                              </span>
                            </button>
                          </th>
                        ))}
                      </tr>
                    ))}
                  </thead>
                  <tbody>
                    {loading && !data
                      ? Array.from({ length: 8 }, (_, i) => (
                          <tr key={i} className="skeleton-row">
                            <td colSpan={columnPairs.length}>
                              <span />
                            </td>
                          </tr>
                        ))
                      : table.getRowModel().rows.map((row) => (
                          <tr
                            key={row.id}
                            data-game-id={row.original.game_id}
                            data-team-id={row.original.team_id}
                          >
                            {row.getVisibleCells().map((cell) => (
                              <td key={cell.id} data-column={cell.column.id}>
                                {flexRender(
                                  cell.column.columnDef.cell,
                                  cell.getContext(),
                                )}
                              </td>
                            ))}
                          </tr>
                        ))}
                  </tbody>
                </table>
                {!loading && !rows.length && (
                  <p className="empty">No games match these filters.</p>
                )}
              </div>
            )}
            <p className="table-note">
              PPP = points per 100 possessions. Green indicates a stronger
              season percentile; red indicates a weaker one. Gameflow will be
              available in a later migration phase.
            </p>
          </section>
        </div>
      </main>
    </>
  );
}
