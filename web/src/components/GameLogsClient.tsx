"use client";
// The Game Logs tab as Shiny draws it: ui_tab4_gamelogs.R (Israeli League) and
// ui_tab11_euro_gamelogs.R (EuroLeague, EuroCup), with the chip bar of
// build_filter_chips() / stat_filter_chips_ui() and the clears of
// setup_chip_clears(). Markup and classes follow the live page so app.css and
// mobile.css style it; static text comes from scripts/export_web_shiny_ui.R.
// Filter state lives in the URL; the API computes the table (gameLogs()).
import {
  useCallback,
  useEffect,
  useMemo,
  useRef,
  useState,
  type ReactNode,
} from "react";
import { parseAsString, useQueryStates } from "nuqs";
import type { League, Row } from "@/rules/gameLogs";
import catalogues from "@/data/catalogues.json";
import ui from "@/data/shinyUi.json";
import Navbar, { seasonLabel } from "./shiny/Navbar";
import Selectize, { type Choice } from "./shiny/Selectize";
import DateRangeInput from "./shiny/DateRangeInput";
import GameLogsTable from "./shiny/GameLogsTable";
import { useShinyBody } from "./shiny/useShinyBody";
import { Tip, TouchTipsContext } from "./shiny/TouchTips";

type Payload = {
  rows: Row[];
  meta: {
    version: string;
    teams: { id: number; name: string }[];
    rounds: number[];
    phases: string[];
    seasons?: string[];
    view?: View;
    shot_averages: Record<string, Record<string, number>> | null;
  };
};
type View = "summary" | "four_factors";
type Filter = { col: string; op: "ge" | "le"; value: number };
const EMPTY: Row[] = [];
const FILTER_TIPS = ui.filterTooltips as Record<string, string>;
const PHASES = catalogues.phases as Record<string, string>;
const GAME_TYPES = catalogues.gameTypes as Record<string, string>;
const IL_TEAMS = ui.ilTeams as Record<string, { id: number; name: string }[]>;
const BOUNDS = ui.seasonDateBounds as Record<
  string,
  { start: string; end: string }
>;

// tt(): a label carrying the FILTER_TOOLTIPS definition (Tip, TouchTips.tsx).
const tt = (label: string, key: string) => (
  <Tip label={label} tip={FILTER_TIPS[key]} />
);

// season_date_bounds_for_year() / euro_season_date_bounds().
export function seasonBounds(league: League, season: string) {
  const y = Number(season);
  if (league === "il")
    return BOUNDS[season] ?? { start: `${y - 1}-09-01`, end: `${y}-07-01` };
  return { start: `${y}-09-01`, end: `${y + 1}-07-01` };
}
// format(start, "%b %d") in build_filter_chips().
const MONTHS = [
  "Jan",
  "Feb",
  "Mar",
  "Apr",
  "May",
  "Jun",
  "Jul",
  "Aug",
  "Sep",
  "Oct",
  "Nov",
  "Dec",
];
const chipDate = (iso: string) =>
  `${MONTHS[Number(iso.slice(5, 7)) - 1]} ${iso.slice(8, 10)}`;

function parseStats(value: string): Filter[] {
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

// Bootstrap's collapse and the explainer toggles of app.js, for the static
// fragments: "Show/Hide" toggles the explainer body and the example box after
// it; "Show/Hide Example" toggles the example box; "Open glossary" opens it.
function useFragmentClicks(
  ref: React.RefObject<HTMLDivElement | null>,
  onGlossary: () => void,
) {
  useEffect(() => {
    const root = ref.current;
    if (!root) return;
    const toggle = (el: Element | null) => el?.classList.toggle("show");
    const onClick = (e: MouseEvent) => {
      const t = e.target as HTMLElement;
      const explainer = t.closest<HTMLElement>(".js-explainer-toggle");
      if (explainer) {
        e.preventDefault();
        toggle(document.getElementById(explainer.dataset.targetId ?? ""));
        let sib =
          explainer.closest(".explainer-card")?.nextElementSibling ?? null;
        while (sib) {
          if (sib.classList.contains("collapse")) {
            toggle(sib);
            break;
          }
          const inner = sib.querySelector(".collapse");
          if (inner) {
            toggle(inner);
            break;
          }
          sib = sib.nextElementSibling;
        }
        return;
      }
      const collapse = t.closest<HTMLElement>("[data-bs-toggle='collapse']");
      if (collapse) {
        e.preventDefault();
        toggle(root.querySelector(collapse.dataset.bsTarget ?? ""));
        return;
      }
      if (t.closest("[data-input-id='open_glossary']")) {
        e.preventDefault();
        onGlossary();
      }
    };
    root.addEventListener("click", onClick);
    return () => root.removeEventListener("click", onClick);
  }, [ref, onGlossary]);
}

// make_chip(): label, optional pencil (focus_id), and the x.
function Chip({
  label,
  focus,
  onClear,
  onFocus,
}: {
  label: string;
  focus?: string;
  onClear: () => void;
  onFocus: (id: string) => void;
}) {
  return (
    <span
      className={`filter-chip chip-game${focus ? " chip-focusable" : ""}`}
      data-chip-focus={focus}
      tabIndex={focus ? 0 : undefined}
      role={focus ? "button" : undefined}
      onClick={(e) => {
        if (focus && !(e.target as HTMLElement).closest(".chip-x"))
          onFocus(focus);
      }}
      onKeyDown={(e) => {
        if (
          focus &&
          (e.key === "Enter" || e.key === " ") &&
          !(e.target as HTMLElement).closest(".chip-x")
        ) {
          e.preventDefault();
          onFocus(focus);
        }
      }}
    >
      {label}
      {focus && <i className="bi bi-pencil chip-edit" aria-hidden="true"></i>}
      <button
        className="chip-x"
        type="button"
        aria-label={`Clear ${label}`}
        onClick={onClear}
      >
        ×
      </button>
    </span>
  );
}

function Glossary({ onClose }: { onClose: () => void }) {
  useEffect(() => {
    document.body.classList.add("modal-open");
    const key = (e: KeyboardEvent) => e.key === "Escape" && onClose();
    window.addEventListener("keydown", key);
    return () => {
      document.body.classList.remove("modal-open");
      window.removeEventListener("keydown", key);
    };
  }, [onClose]);
  return (
    <div id="shiny-modal-wrapper">
      <div
        id="shiny-modal"
        className="modal fade show"
        tabIndex={-1}
        role="dialog"
        aria-modal="true"
        aria-labelledby="glossary-title"
        style={{ display: "block" }}
        onClick={(e) => e.target === e.currentTarget && onClose()}
      >
        <div className="modal-dialog modal-lg" role="document">
          <div className="modal-content">
            <div className="modal-header">
              <h4 className="modal-title" id="glossary-title">
                Glossary
              </h4>
            </div>
            <div
              className="modal-body"
              dangerouslySetInnerHTML={{ __html: ui.glossaryHtml }}
            />
            <div className="modal-footer">
              <button
                type="button"
                className="btn btn-default"
                onClick={onClose}
                autoFocus
              >
                Close
              </button>
            </div>
          </div>
        </div>
      </div>
      <div className="modal-backdrop fade show"></div>
    </div>
  );
}

export default function GameLogsClient({ league }: { league: League }) {
  const euro = league !== "il";
  const p = euro ? "eurogl" : "gl";
  const defaultSeason = euro ? ui.euroDefaultSeason : ui.defaultGameYear;
  const [query, setQuery] = useQueryStates(
    {
      season: parseAsString.withDefault(defaultSeason),
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
  const view: View = query.view === "four_factors" ? "four_factors" : "summary";
  const stats = euro ? [] : parseStats(query.stats);
  const bounds = seasonBounds(league, query.season);
  const body = useShinyBody(euro);

  const [data, setData] = useState<Payload | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
  const [revision, setRevision] = useState(0);
  // Each answer redraws the table from page 1 in its default order, as a new
  // DT::datatable() does in Shiny.
  const [draw, setDraw] = useState(0);
  const [updated, setUpdated] = useState<string | null>(null);
  const [glossary, setGlossary] = useState(false);
  const [filtersShown, setFiltersShown] = useState(false);
  const [accordionOpen, setAccordionOpen] = useState(true);
  const [statOpen, setStatOpen] = useState(false);
  const [statCol, setStatCol] = useState("");
  const [statOp, setStatOp] = useState<"ge" | "le">("ge");
  const [statValue, setStatValue] = useState("");
  const version = useRef("");
  const fragments = useRef<HTMLDivElement>(null);
  const openGlossary = useCallback(() => setGlossary(true), [setGlossary]);
  useFragmentClicks(fragments, openGlossary);

  // The API request: only complete starter bounds, only a set date range.
  const params = useMemo(() => {
    const q = new URLSearchParams();
    for (const [key, value] of Object.entries(query)) {
      if (!value) continue;
      if (
        (key === "own_starters" || key === "opp_starters") &&
        !/^(gte|lte):[0-5]$/.test(value)
      )
        continue;
      if (euro && (key === "team" || key === "stats" || key === "game_type"))
        continue;
      if (!euro && (key === "teams" || key === "phase")) continue;
      q.set(key, value);
    }
    return q.toString();
  }, [query, euro]);

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
            setDraw((d) => d + 1);
            version.current = answer.meta.version;
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

  // The navbar stamp is the Israeli ETL's last success (output$last_updated);
  // a new version of this league's data refetches the table.
  useEffect(() => {
    const check = () => {
      void fetch("/api/v1/version")
        .then((r) => (r.ok ? r.json() : null))
        .then((value) => {
          const list = (value?.leagues ?? []) as {
            league: string;
            version: string;
          }[];
          const il = list.find((x) => x.league === "il")?.version;
          if (il && il !== "unknown") setUpdated(il);
          const v = list.find((x) => x.league === league)?.version;
          if (v && version.current && v !== version.current) {
            version.current = v;
            setRevision((r) => r + 1);
          }
        })
        .catch(() => {});
    };
    check();
    const timer = setInterval(check, 60000);
    window.addEventListener("focus", check);
    return () => {
      clearInterval(timer);
      window.removeEventListener("focus", check);
    };
  }, [league]);

  const teams: Choice[] = useMemo(() => {
    const list = (!euro && IL_TEAMS[query.season]) || data?.meta.teams || [];
    return list.map((t) => ({ value: String(t.id), label: t.name }));
  }, [euro, query.season, data?.meta.teams]);
  const teamName = (id: string) =>
    teams.find((t) => t.value === id)?.label ?? id;
  const rounds = data?.meta.rounds ?? [];
  const roundChoices = rounds.map((r) => ({
    value: String(r),
    label: String(r),
  }));
  const lastChoices = rounds.length
    ? Array.from({ length: Math.max(...rounds) }, (_, i) => ({
        value: String(i + 1),
        label: String(i + 1),
      }))
    : [];
  const typeChoices = euro
    ? (data?.meta.phases ?? []).map((ph) => ({
        value: ph,
        label: PHASES[ph] ?? ph,
      }))
    : catalogues.gameTypeChoices.filter((c) => c.value !== "");
  const seasons = euro
    ? (data?.meta.seasons ?? ui.euroStaticSeasons)
    : ui.ilSeasons;
  const statCols = ui.statFilterCols[view];

  type Key = keyof typeof query;
  const set = (patch: Partial<Record<Key, string | null>>) => {
    // setup_gn_last_n_sync(): Last N and a GN range exclude each other.
    const next: Partial<Record<Key, string | null>> = { ...patch };
    if (patch.last_n) Object.assign(next, { min_gn: null, max_gn: null });
    if (patch.min_gn || patch.max_gn) next.last_n = null;
    for (const k of Object.keys(next) as Key[])
      if (next[k] === "") next[k] = null;
    void setQuery(next);
  };
  const list = (v: string) => v.split(",").filter(Boolean);
  const typeKey: Key = euro ? "phase" : "game_type";
  const teamKey: Key = euro ? "teams" : "team";

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
  const changeSeason = (season: string) =>
    set({
      season,
      start: null,
      end: null,
      min_gn: null,
      max_gn: null,
      last_n: null,
      ...(euro ? {} : { team: null }),
    });
  const changeView = (v: "Summary" | "Four Factors") =>
    set({ view: v === "Four Factors" ? "four_factors" : null, stats: null });

  // app.js chip focus: open the panel, the accordion, scroll and highlight.
  const reveal = (id: string) => {
    if (body.filtersCollapsed) body.toggleFilters();
    if (window.matchMedia("(max-width: 767.98px)").matches)
      setFiltersShown(true);
    setAccordionOpen(true);
    window.setTimeout(() => {
      const el =
        document.getElementById(id) ??
        document.querySelector<HTMLElement>(`[data-input-id="${id}"]`);
      if (!el) return;
      const group =
        el.closest<HTMLElement>(".form-group, .shiny-input-container") ?? el;
      group.scrollIntoView({ block: "center", behavior: "smooth" });
      group.classList.add("ibpl-chip-revealed");
      window.setTimeout(
        () => group.classList.remove("ibpl-chip-revealed"),
        2400,
      );
      const input = group.querySelector<HTMLInputElement>(
        ".selectize-input input, input",
      );
      input?.focus({ preventScroll: true });
    }, 80);
  };

  const starters = (key: "own_starters" | "opp_starters") => {
    const [mode = "", value = ""] = query[key].split(":");
    return { mode, value };
  };
  const starterRow = (
    key: "own_starters" | "opp_starters",
    side: "off" | "def",
  ) => {
    const { mode, value } = starters(key);
    const own = side === "off";
    return (
      <div className="row">
        <div className="col-sm-6">
          <Selectize
            id={`${p}_num_starters_${side}_mode`}
            formControl={false}
            label={tt(
              own ? "Own lineup starters" : "Opponent lineup starters",
              own ? "own_starters" : "opp_starters",
            )}
            placeholder="ALL"
            choices={[
              { value: "gte", label: "At least (>=)" },
              { value: "lte", label: "At most (<=)" },
            ]}
            emptyLabel="ALL"
            value={[mode]}
            onChange={(v) =>
              set({ [key]: v[0] || value ? `${v[0] ?? ""}:${value}` : null })
            }
          />
        </div>
        <div className="col-sm-6">
          <Selectize
            id={`${p}_num_starters_${side}`}
            formControl={false}
            label={own ? "Own value" : "Opp value"}
            placeholder="--"
            choices={["0", "1", "2", "3", "4", "5"].map((n) => ({
              value: n,
              label: n,
            }))}
            emptyLabel="--"
            value={[value]}
            onChange={(v) =>
              set({ [key]: mode || v[0] ? `${mode}:${v[0] ?? ""}` : null })
            }
          />
        </div>
      </div>
    );
  };

  // build_filter_chips(): the season chip, then each active filter.
  const chips: ReactNode[] = [];
  const start = query.start || bounds.start,
    end = query.end || bounds.end;
  const datesShown = euro ? start !== bounds.start || end !== bounds.end : true;
  if (datesShown)
    chips.push(
      <Chip
        key="dates"
        label={`${chipDate(start)} - ${chipDate(end)}`}
        focus={`${p}_dates`}
        onClear={() => set({ start: null, end: null })}
        onFocus={reveal}
      />,
    );
  const types = list(query[typeKey]);
  if (types.length)
    chips.push(
      <Chip
        key="type"
        focus={`${p}_${euro ? "phase" : "game_type"}`}
        onFocus={reveal}
        label={types
          .map((t) => (euro ? (PHASES[t] ?? t) : (GAME_TYPES[t] ?? t)))
          .join(", ")}
        onClear={() => set({ [typeKey]: null })}
      />,
    );
  const picked = list(query[teamKey]);
  if (picked.length)
    chips.push(
      <Chip
        key="teams"
        focus={euro ? `${p}_teams` : `${p}_team`}
        onFocus={reveal}
        label={
          picked.length === 1 ? teamName(picked[0]) : `${picked.length} teams`
        }
        onClear={() => set({ [teamKey]: null })}
      />,
    );
  const opps = list(query.opponents);
  if (opps.length)
    chips.push(
      <Chip
        key="opps"
        focus={`${p}_opponents`}
        onFocus={reveal}
        label={
          opps.length === 1
            ? `vs ${teamName(opps[0])}`
            : `vs ${opps.length} opps`
        }
        onClear={() => set({ opponents: null })}
      />,
    );
  if (query.home_away)
    chips.push(
      <Chip
        key="ha"
        focus={`${p}_home_away`}
        onFocus={reveal}
        label={query.home_away === "home" ? "Home" : "Away"}
        onClear={() => set({ home_away: null })}
      />,
    );
  if (query.outcome)
    chips.push(
      <Chip
        key="out"
        focus={`${p}_outcome`}
        onFocus={reveal}
        label={query.outcome === "win" ? "Wins" : "Losses"}
        onClear={() => set({ outcome: null })}
      />,
    );
  if (query.min_gn || query.max_gn) {
    const gl = euro ? "Rd" : "GN";
    const parts = [
      query.min_gn && `${gl}>=${query.min_gn}`,
      query.max_gn && `${gl}<=${query.max_gn}`,
    ].filter(Boolean);
    chips.push(
      <Chip
        key="gn"
        focus={`${p}_gn_min`}
        onFocus={reveal}
        label={parts.join(" ")}
        onClear={() => set({ min_gn: null, max_gn: null })}
      />,
    );
  }
  if (query.last_n)
    chips.push(
      <Chip
        key="last"
        focus={`${p}_last_n`}
        onFocus={reveal}
        label={`Last ${query.last_n} games`}
        onClear={() => set({ last_n: null })}
      />,
    );
  const sp = (["own_starters", "opp_starters"] as const)
    .map((k) => {
      const { mode, value } = starters(k);
      return mode && value
        ? `${k === "own_starters" ? "Own" : "Opp"} ${mode === "gte" ? ">=" : "<="}${value}`
        : "";
    })
    .filter(Boolean);
  if (sp.length)
    chips.push(
      <Chip
        key="st"
        label={`Starters: ${sp.join(", ")}`}
        onFocus={reveal}
        onClear={() => set({ own_starters: null, opp_starters: null })}
      />,
    );

  // Popper (bslib popover) keeps the popover inside the viewport; the arrow
  // stays over the chip.
  const keepInViewport = (el: HTMLDivElement | null) => {
    if (!el) return;
    el.style.transform = "translateX(-50%)";
    const r = el.getBoundingClientRect();
    const shift =
      r.right > innerWidth - 8
        ? innerWidth - 8 - r.right
        : r.left < 8
          ? 8 - r.left
          : 0;
    el.style.transform = `translateX(calc(-50% + ${shift}px))`;
    const arrow = el.querySelector<HTMLElement>(".popover-arrow");
    if (arrow) arrow.style.transform = `translateX(calc(-50% - ${shift}px))`;
  };
  const addStat = () => {
    const col = statCols.find((c) => c.label === statCol);
    const value = Number(statValue);
    if (!col || statValue.trim() === "" || !Number.isFinite(value)) return;
    set({
      stats: JSON.stringify([...stats, { col: col.col, op: statOp, value }]),
    });
    setStatCol("");
    setStatOp("ge");
    setStatValue("");
  };
  const statLabel = (col: string) =>
    statCols.find((c) => c.col === col)?.label ?? col;

  const rows = data?.rows ?? EMPTY;
  const html = euro ? ui.euro : ui.il;
  const dateLabel = euro ? "Game Date Range" : "Date range";
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
  const dates = (
    <DateRangeInput
      id={`${p}_dates`}
      label={dateLabel}
      start={start}
      end={end}
      min={bounds.start}
      max={bounds.end}
      onChange={(s, e) =>
        set({
          start: s === bounds.start && e === bounds.end ? null : s,
          end: s === bounds.start && e === bounds.end ? null : e,
        })
      }
    />
  );

  return (
    <TouchTipsContext.Provider value={body.touchTips}>
      <Navbar
        league={league}
        season={query.season}
        seasons={seasons}
        view={view === "four_factors" ? "Four Factors" : "Summary"}
        onView={changeView}
        onSeason={changeSeason}
        onGlossary={openGlossary}
        updated={updated}
        collapsedNav={body.collapsedNav}
      >
        <div className="tab-content">
          <div
            className="tab-pane active show"
            data-value={euro ? "euro_game_logs" : "game_logs"}
            role="tabpanel"
          >
            <div className="container-fluid">
              <div className="row">
                <div className="col-sm-3 ibpl-filter-col">
                  <form
                    className="well"
                    role="complementary"
                    onSubmit={(e) => e.preventDefault()}
                  >
                    <div className="view-mode-container">
                      <div
                        className="form-group shiny-input-radiogroup shiny-input-container shiny-input-container-inline"
                        role="radiogroup"
                        aria-label="View"
                      >
                        <label className="control-label">View:</label>
                        <div className="shiny-options-group">
                          {(["Summary", "Four Factors"] as const).map((v) => (
                            <label
                              key={v}
                              className="radio-inline"
                              data-tooltip={
                                v === "Summary"
                                  ? FILTER_TIPS.view_summary
                                  : FILTER_TIPS.view_ff
                              }
                            >
                              <input
                                type="radio"
                                name={`${p}_view_mode`}
                                value={v}
                                checked={
                                  (view === "four_factors") ===
                                  (v === "Four Factors")
                                }
                                onChange={() => changeView(v)}
                              />
                              <span>{v}</span>
                            </label>
                          ))}
                        </div>
                      </div>
                    </div>
                    <hr />
                    <button
                      type="button"
                      className="btn btn-outline-secondary d-md-none w-100 mb-2"
                      aria-expanded={filtersShown}
                      onClick={() => setFiltersShown((s) => !s)}
                    >
                      Show Filters
                    </button>
                    <div
                      id={`${p}-filters`}
                      className={`collapse d-md-block${filtersShown ? " show" : ""}`}
                    >
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
                      {starterRow("own_starters", "off")}
                      {starterRow("opp_starters", "def")}
                      <hr />
                      <div className="text-end mb-2">
                        <a
                          href="#"
                          className="small text-muted fw-bold js-accordion-toggle-all"
                          style={{ textDecoration: "none" }}
                          onClick={(e) => {
                            e.preventDefault();
                            setAccordionOpen((o) => !o);
                          }}
                        >
                          Collapse/Expand All
                        </a>
                      </div>
                      <div className="accordion">
                        <div
                          className="accordion-item"
                          data-value="Game Filters"
                        >
                          <div className="accordion-header">
                            <button
                              className={`accordion-button${accordionOpen ? "" : " collapsed"}`}
                              type="button"
                              aria-expanded={accordionOpen}
                              aria-controls={`${p}-game-filters`}
                              onClick={() => setAccordionOpen((o) => !o)}
                            >
                              <div className="accordion-icon"></div>
                              <div className="accordion-title">
                                Game Filters
                              </div>
                            </button>
                          </div>
                          <div
                            id={`${p}-game-filters`}
                            className={`accordion-collapse collapse${accordionOpen ? " show" : ""}`}
                          >
                            <div className="accordion-body">
                              <Selectize
                                id={`${p}_${euro ? "phase" : "game_type"}`}
                                label={euro ? "Phase" : "Game type"}
                                multiple
                                placeholder={
                                  euro ? "All phases" : "All game types"
                                }
                                choices={typeChoices}
                                value={types}
                                onChange={(v) =>
                                  set({ [typeKey]: v.join(",") })
                                }
                              />
                              <Selectize
                                id={`${p}_opponents`}
                                label="Opponents"
                                multiple
                                placeholder="All opponents"
                                choices={teams}
                                value={opps}
                                onChange={(v) =>
                                  set({ opponents: v.join(",") })
                                }
                              />
                              <Selectize
                                id={`${p}_home_away`}
                                label="Home/Away"
                                formControl={false}
                                placeholder="All"
                                emptyLabel="All"
                                choices={[
                                  { value: "home", label: "Home" },
                                  { value: "away", label: "Away" },
                                ]}
                                value={[query.home_away]}
                                onChange={(v) =>
                                  set({ home_away: v[0] ?? null })
                                }
                              />
                              <Selectize
                                id={`${p}_outcome`}
                                label="Outcome"
                                formControl={false}
                                placeholder="All"
                                emptyLabel="All"
                                choices={[
                                  { value: "win", label: "Win" },
                                  { value: "loss", label: "Loss" },
                                ]}
                                value={[query.outcome]}
                                onChange={(v) => set({ outcome: v[0] ?? null })}
                              />
                              <hr />
                              <div className="row">
                                <div className="col-sm-6">
                                  <Selectize
                                    id={`${p}_gn_min`}
                                    label={
                                      euro
                                        ? "From Round"
                                        : tt("From Game Number (GN)", "gn")
                                    }
                                    placeholder="Any"
                                    choices={roundChoices}
                                    value={[query.min_gn]}
                                    onChange={(v) =>
                                      set({ min_gn: v[0] ?? null })
                                    }
                                  />
                                </div>
                                <div className="col-sm-6">
                                  <Selectize
                                    id={`${p}_gn_max`}
                                    label={
                                      euro
                                        ? "To Round"
                                        : tt("To Game Number (GN)", "gn")
                                    }
                                    placeholder="Any"
                                    choices={roundChoices}
                                    value={[query.max_gn]}
                                    onChange={(v) =>
                                      set({ max_gn: v[0] ?? null })
                                    }
                                  />
                                </div>
                              </div>
                              <Selectize
                                id={`${p}_last_n`}
                                label={tt("Last N Team Games", "last_n")}
                                placeholder="Any"
                                choices={lastChoices}
                                value={[query.last_n]}
                                onChange={(v) => set({ last_n: v[0] ?? null })}
                              />
                            </div>
                          </div>
                        </div>
                      </div>
                    </div>
                  </form>
                </div>
                <div className="col-sm-9 ibpl-main-col" role="main">
                  <div ref={fragments}>
                    <div
                      className="shiny-panel-conditional"
                      style={
                        view === "summary" ? undefined : { display: "none" }
                      }
                      dangerouslySetInnerHTML={{ __html: html.summary }}
                    />
                    <div
                      className="shiny-panel-conditional"
                      style={
                        view === "four_factors"
                          ? undefined
                          : { display: "none" }
                      }
                      dangerouslySetInnerHTML={{ __html: html.four_factors }}
                    />
                  </div>
                  <div className="chips-row">
                    <button
                      type="button"
                      className="chips-filters-toggle js-filters-toggle"
                      aria-expanded={!body.filtersCollapsed}
                      aria-label={
                        body.filtersCollapsed
                          ? "Show the filter panel"
                          : "Hide the filter panel"
                      }
                      onClick={body.toggleFilters}
                    >
                      <i className="bi bi-sliders" aria-hidden="true"></i>
                      <span className="chips-filters-toggle-label">
                        Filters
                      </span>
                    </button>
                    <div className="chips-row-chips">
                      <div id={`${p}_filter_chips`} aria-live="polite">
                        <div className="filter-chips">
                          <span className="filter-chip chip-season">
                            {euro
                              ? `${ui.euroCompetitionLabels[league === "euroleague" ? "E" : "U"]} ${seasonLabel(query.season, false)}`
                              : seasonLabel(query.season)}
                          </span>
                          {chips}
                          {!euro &&
                            stats.map((f, i) => (
                              <span
                                key={`${f.col}-${i}`}
                                className="filter-chip chip-stat"
                              >
                                {`${statLabel(f.col)} ${f.op === "ge" ? ">=" : "<="} ${f.value.toLocaleString("en-US", { maximumFractionDigits: 7 })}`}{" "}
                                <a
                                  href="#"
                                  style={{ marginLeft: 4, color: "inherit" }}
                                  aria-label={`Remove ${statLabel(f.col)} filter`}
                                  onClick={(e) => {
                                    e.preventDefault();
                                    const rest = stats.filter(
                                      (_, j) => j !== i,
                                    );
                                    set({
                                      stats: rest.length
                                        ? JSON.stringify(rest)
                                        : null,
                                    });
                                  }}
                                >
                                  ×
                                </a>
                              </span>
                            ))}
                          {!euro && (
                            <span
                              style={{
                                position: "relative",
                                display: "inline-block",
                              }}
                            >
                              <span
                                className="filter-chip filter-chip-add"
                                id={`${p}_stat_filter_add_btn`}
                                role="button"
                                tabIndex={0}
                                aria-pressed={statOpen}
                                aria-expanded={statOpen}
                                style={{ cursor: "pointer" }}
                                onClick={() => setStatOpen((o) => !o)}
                                onKeyDown={(e) => {
                                  if (e.key === "Enter" || e.key === " ") {
                                    e.preventDefault();
                                    setStatOpen((o) => !o);
                                  }
                                }}
                              >
                                <i className="bi bi-plus"></i> Stat filter
                              </span>
                              {statOpen && (
                                <div
                                  className="popover bs-popover-auto fade show"
                                  role="dialog"
                                  aria-label="Add stat filter"
                                  data-popper-placement="bottom"
                                  ref={keepInViewport}
                                  style={{
                                    position: "absolute",
                                    top: "100%",
                                    left: "50%",
                                    transform: "translateX(-50%)",
                                    marginTop: 8,
                                  }}
                                >
                                  <div
                                    className="popover-arrow"
                                    style={{
                                      position: "absolute",
                                      left: "50%",
                                      transform: "translateX(-50%)",
                                    }}
                                  ></div>
                                  <h3 className="popover-header">
                                    Add stat filter
                                  </h3>
                                  <div className="popover-body">
                                    <div
                                      className={`${p}-stat-popover`}
                                      style={{ minWidth: 220 }}
                                    >
                                      <Selectize
                                        id={`${p}_stat_filter_col`}
                                        label="Column"
                                        formControl={false}
                                        placeholder="Choose..."
                                        style={{ width: "100%" }}
                                        choices={statCols.map((c) => ({
                                          value: c.label,
                                          label: c.label,
                                        }))}
                                        value={[statCol]}
                                        onChange={(v) => setStatCol(v[0] ?? "")}
                                      />
                                      <div
                                        className="form-group shiny-input-radiogroup shiny-input-container shiny-input-container-inline"
                                        role="radiogroup"
                                        aria-labelledby={`${p}_stat_filter_op-label`}
                                      >
                                        <label
                                          className="control-label"
                                          id={`${p}_stat_filter_op-label`}
                                        >
                                          Operator
                                        </label>
                                        <div className="shiny-options-group">
                                          {(["ge", "le"] as const).map((op) => (
                                            <label
                                              key={op}
                                              className="radio-inline"
                                            >
                                              <input
                                                type="radio"
                                                name={`${p}_stat_filter_op`}
                                                value={op}
                                                checked={statOp === op}
                                                onChange={() => setStatOp(op)}
                                              />
                                              <span>
                                                {op === "ge" ? ">=" : "<="}
                                              </span>
                                            </label>
                                          ))}
                                        </div>
                                      </div>
                                      <div
                                        className="form-group shiny-input-container"
                                        style={{ width: "100%" }}
                                      >
                                        <label
                                          className="control-label"
                                          htmlFor={`${p}_stat_filter_value`}
                                        >
                                          Value
                                        </label>
                                        <input
                                          id={`${p}_stat_filter_value`}
                                          type="number"
                                          className="shiny-input-number form-control"
                                          value={statValue}
                                          onChange={(e) =>
                                            setStatValue(e.target.value)
                                          }
                                          onKeyDown={(e) =>
                                            e.key === "Enter" && addStat()
                                          }
                                        />
                                      </div>
                                      {statCols.some((c) =>
                                        c.label.includes("%"),
                                      ) && (
                                        <div className="small text-muted mb-2">
                                          Percent columns: enter as 0-100.
                                        </div>
                                      )}
                                      <button
                                        className="btn btn-default action-button btn-sm btn-primary w-100"
                                        type="button"
                                        onClick={addStat}
                                      >
                                        Add
                                      </button>
                                    </div>
                                    <button
                                      type="button"
                                      aria-label="Close"
                                      className="btn-close"
                                      onClick={() => setStatOpen(false)}
                                      style={{
                                        position: "absolute",
                                        top: "0.6rem",
                                        right: "0.25rem",
                                        width: "0.55rem",
                                        height: "0.55rem",
                                        backgroundSize: "0.55rem",
                                      }}
                                    ></button>
                                  </div>
                                </div>
                              )}
                            </span>
                          )}
                          {(chips.length > 0 || stats.length > 0) && (
                            <button
                              className="chip-clear-all"
                              type="button"
                              onClick={reset}
                            >
                              Clear all
                            </button>
                          )}
                        </div>
                      </div>
                    </div>
                  </div>
                  <div dangerouslySetInnerHTML={{ __html: html.hint }} />
                  <div
                    id={`${p}_table`}
                    className={`datatables html-widget html-widget-output${loading ? " recalculating" : ""}`}
                    style={{ width: "100%", height: "auto" }}
                    aria-busy={loading}
                    aria-live="polite"
                  >
                    {error ? (
                      <div className="shiny-output-error" role="alert">
                        <span>{error}</span>{" "}
                        <button
                          type="button"
                          className="btn btn-default btn-sm"
                          onClick={() => setRevision((r) => r + 1)}
                        >
                          Retry
                        </button>
                      </div>
                    ) : !data ? null : rows.length ? (
                      <GameLogsTable
                        key={draw}
                        league={league}
                        view={data.meta.view ?? view}
                        rows={rows}
                        shotAverages={data.meta.shot_averages}
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
                  </div>
                </div>
              </div>
            </div>
          </div>
        </div>
      </Navbar>
      {glossary && <Glossary onClose={() => setGlossary(false)} />}
    </TouchTipsContext.Provider>
  );
}
