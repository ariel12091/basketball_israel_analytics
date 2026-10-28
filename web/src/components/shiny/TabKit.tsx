"use client";
// The parts every migrated Shiny tab shares, so each tab file only describes
// what is its own. Each piece names the Shiny code it reproduces:
// - useTabQuery: the tab's inputs, kept in the URL (bookmark restore, X-18).
// - useTabData / useVersion: one API call per filter state (debounced,
//   superseded requests aborted), refetched when the data version moves.
// - TabFrame: navbarPage + sidebarLayout, the glossary modal, touch tooltips.
// - StarterRows, GameFiltersAccordion: starter_context_filters_ui() and
//   game_context_filters_ui() (with the Opponent Strength panel).
// - contextChips, Chip, StatFilters, ChipsRow: build_filter_chips(),
//   make_chip(), stat_filter_chips_ui(), filter_chips_row().
// - ViewContent: the static explainer/example/legend blocks (shiny/Blocks).
import {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useRef,
  useState,
  type ReactNode,
} from "react";
import { parseAsString, useQueryStates } from "nuqs";
import { pollVersions } from "@/lib/versionPolling";
import { leagueOf, noteVersion, withVersion } from "@/lib/dataVersion";
import {
  contextParams,
  parseStats,
  queryDefaults,
  seasonBounds,
  type Patch,
  type Query,
  type StatFilter,
} from "@/lib/tabQuery";
export { contextParams, parseStats, seasonBounds };
export type { Patch, Query, StatFilter };
import type { League } from "@/rules/gameLogs";
import catalogues from "@/data/catalogues.json";
import ui from "@/data/shinyUi.json";
import Navbar, { seasonLabel, type TabId } from "./Navbar";
import Selectize, { type Choice } from "./Selectize";
import { Tip, TouchTipsContext } from "./TouchTips";
import { useShinyBody } from "./useShinyBody";
import { TextBlocks, ViewBlocks, type Block, type TextBlock } from "./Blocks";
import FilterDrawer, { type DrawerSection } from "./FilterDrawer";
export type { DrawerSection } from "./FilterDrawer";

export const FILTER_TIPS = ui.filterTooltips as Record<string, string>;
export const PHASES = catalogues.phases as Record<string, string>;
export const GAME_TYPES = catalogues.gameTypes as Record<string, string>;
const IL_TEAMS = ui.ilTeams as Record<string, { id: number; name: string }[]>;

/** tt(): a label with its FILTER_TOOLTIPS definition. */
export const tt = (label: string, key: string) => (
  <Tip label={label} tip={FILTER_TIPS[key]} />
);

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
/** format(d, "%b %d"). */
export const chipDate = (iso: string) =>
  `${MONTHS[Number(iso.slice(5, 7)) - 1]} ${iso.slice(8, 10)}`;
export const list = (v: string) => v.split(",").filter(Boolean);

// ---- URL state ---------------------------------------------------------
export function useTabQuery(
  league: League,
  extra: Record<string, string> = {},
) {
  const parsers = useMemo(
    () =>
      Object.fromEntries(
        Object.entries(queryDefaults(league, extra)).map(([k, v]) => [
          k,
          parseAsString.withDefault(v),
        ]),
      ),
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [league],
  );
  const [query, setQuery] = useQueryStates(parsers, {
    history: "replace",
    shallow: true,
  });
  const set = useCallback(
    (patch: Patch) => {
      // setup_gn_last_n_sync(): Last N and a GN range exclude each other.
      const next: Patch = { ...patch };
      if (patch.last_n) Object.assign(next, { min_gn: null, max_gn: null });
      if (patch.min_gn || patch.max_gn) next.last_n = null;
      for (const k of Object.keys(next)) if (next[k] === "") next[k] = null;
      void setQuery(next);
    },
    [setQuery],
  );
  return { query: query as Query, set };
}

// ---- Data ----------------------------------------------------------------
/** `initial`: the answer the server rendered for `initial.url`; the first
 * request is skipped while the URL still matches it. */
export function useTabData<T extends { meta: { version: string } }>(
  url: string | null,
  initial?: { url: string; data: T | null },
) {
  const seeded = initial?.data != null && initial.url === url;
  const [data, setData] = useState<T | null>(seeded ? initial!.data : null);
  const [loading, setLoading] = useState(!seeded);
  const skip = useRef(seeded);
  const [error, setError] = useState("");
  const [revision, setRevision] = useState(0);
  // Each answer redraws its table from page 1 in the default order, as a new
  // DT::datatable() does in Shiny.
  const [draw, setDraw] = useState(0);
  const version = useRef(seeded ? initial!.data!.meta.version : "");
  if (seeded && url) noteVersion(leagueOf(url), initial!.data!.meta.version);
  useEffect(() => {
    if (!url) return;
    if (skip.current) {
      skip.current = false;
      return;
    }
    const controller = new AbortController();
    let active = true;
    const timer = setTimeout(() => {
      setLoading(true);
      setError("");
      void fetch(withVersion(url), { signal: controller.signal })
        .then(async (response) => {
          const answer = await response.json();
          if (!response.ok)
            throw new Error(answer.error ?? "Unable to load data");
          if (active) {
            setData(answer);
            setDraw((d) => d + 1);
            version.current = answer.meta.version;
            noteVersion(leagueOf(url), answer.meta.version);
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
  }, [url, revision]);
  const retry = useCallback(() => setRevision((r) => r + 1), []);
  return { data, loading, error, draw, retry, version };
}

/** The navbar stamp is the Israeli ETL's last success (output$last_updated);
 * a new version of this league's data refetches. */
export function useVersion(
  league: League,
  version: { current: string },
  refetch: () => void,
) {
  const [updated, setUpdated] = useState<string | null>(null);
  useEffect(
    () =>
      pollVersions((value) => {
        const versions = (value?.leagues ?? []) as {
          league: string;
          version: string;
        }[];
        const il = versions.find((x) => x.league === "il")?.version;
        if (il && il !== "unknown") setUpdated(il);
        const v = versions.find((x) => x.league === league)?.version;
        if (v && version.current && v !== version.current) {
          version.current = v;
          refetch();
        }
      }),
    [league, version, refetch],
  );
  return updated;
}

/** fetch_teams_distinct()/fetch_teams_min() (the catalogue roster) for the
 * Israeli league; the API's euro_fetch_teams() for EuroLeague and EuroCup. */
export function teamChoices(
  league: League,
  season: string,
  apiTeams?: { id: number; name: string }[],
): Choice[] {
  const teams = (league === "il" && IL_TEAMS[season]) || apiTeams || [];
  return teams.map((t) => ({ value: String(t.id), label: t.name }));
}

// ---- Frame ---------------------------------------------------------------
type Frame = {
  layout: "sidebar" | "drawer";
  drawerOpen: boolean;
  setDrawerOpen: (open: boolean) => void;
  reveal: (id: string) => void;
  openGlossary: () => void;
  accordionOpen: boolean;
  setAccordionOpen: (fn: (o: boolean) => boolean) => void;
  filtersCollapsed: boolean;
  toggleFilters: () => void;
};
const FrameContext = createContext<Frame | null>(null);
export const useFrame = () => useContext(FrameContext)!;

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
            <div className="modal-body">
              <TextBlocks blocks={ui.glossary as TextBlock[]} />
            </div>
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

/** navbarPage(...) + fluidPage(sidebarLayout(sidebarPanel(width = 3),
 * mainPanel(width = 9))). `sidebar` receives the mobile "Show Filters"
 * state; the reset button, filters and accordion go inside it. */
export function TabFrame({
  league,
  tab,
  paneValue,
  season,
  seasons,
  onSeason,
  views,
  view,
  onView,
  updated,
  filtersId,
  sidebarTop,
  sidebar,
  main,
  mainOnly = false,
  layout = "sidebar",
  drawer,
}: {
  league: League;
  tab: TabId;
  paneValue: string;
  season: string;
  seasons: string[];
  onSeason: (s: string) => void;
  views: string[];
  view: string;
  onView: (v: string) => void;
  updated: string | null;
  filtersId: string;
  sidebarTop?: ReactNode;
  sidebar: ReactNode;
  main: ReactNode;
  mainOnly?: boolean;
  /** "drawer": no sidebar; the filters live in a right-side drawer opened
   * from the chips row's Filters button (design 2026-10-28 § 2). */
  layout?: "sidebar" | "drawer";
  drawer?: {
    sections: DrawerSection[];
    footer?: ReactNode;
    activeCount: number;
    resettable?: boolean;
    onClearAll: () => void;
    resultLabel?: string;
  };
}) {
  const body = useShinyBody(league !== "il");
  const drawerLayout = layout === "drawer" && !!drawer;
  const [drawerOpen, setDrawerOpen] = useState(false);
  // Every section starts open (user 2026-10-28): the drawer shows all filters.
  const [openSections, setOpenSections] = useState<string[]>(() =>
    (drawer?.sections ?? []).map((s) => s.id),
  );
  const [glossary, setGlossary] = useState(false);
  const [filtersShown, setFiltersShown] = useState(false);
  const [accordionOpen, setAccordionOpen] = useState(true);
  const openGlossary = useCallback(() => setGlossary(true), [setGlossary]);
  // app.js chip focus: open the panel and the accordion, scroll, highlight.
  const reveal = useCallback(
    (id: string) => {
      if (drawerLayout) {
        // The drawer is always mounted: open it and the section holding id.
        setDrawerOpen(true);
        const el =
          document.getElementById(id) ??
          document.querySelector<HTMLElement>(`[data-input-id="${id}"]`);
        const section = el
          ?.closest<HTMLElement>("[data-section]")
          ?.getAttribute("data-section");
        if (section)
          setOpenSections((s) => (s.includes(section) ? s : [...s, section]));
      } else if (body.filtersCollapsed) body.toggleFilters();
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
        // The drawer may still be laying out its controls on the first try.
        window.setTimeout(() => {
          if (input && document.activeElement !== input)
            input.focus({ preventScroll: true });
        }, 200);
      }, 80);
    },
    [body, drawerLayout],
  );
  const frame: Frame = {
    layout: drawerLayout ? "drawer" : "sidebar",
    drawerOpen,
    setDrawerOpen,
    reveal,
    openGlossary,
    accordionOpen,
    setAccordionOpen,
    filtersCollapsed: body.filtersCollapsed,
    toggleFilters: body.toggleFilters,
  };
  return (
    <TouchTipsContext.Provider value={body.touchTips}>
      <FrameContext.Provider value={frame}>
        <Navbar
          league={league}
          tab={tab}
          season={season}
          seasons={seasons}
          views={views}
          view={view}
          onView={onView}
          onSeason={onSeason}
          onGlossary={openGlossary}
          updated={updated}
          collapsedNav={body.collapsedNav}
        >
          <div className="tab-content">
            <div
              className="tab-pane active show"
              data-value={paneValue}
              role="tabpanel"
            >
              <div
                className={`container-fluid${drawerLayout ? ` ibpl-drawer-layout${drawerOpen ? " ibpl-drawer-open" : ""}` : ""}`}
              >
                <div className="row">
                  {!mainOnly && !drawerLayout && (
                    <div className="col-sm-3 ibpl-filter-col">
                      <form
                        className="well"
                        role="complementary"
                        onSubmit={(e) => e.preventDefault()}
                      >
                        {sidebarTop}
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
                          id={filtersId}
                          className={`collapse d-md-block${filtersShown ? " show" : ""}`}
                        >
                          {sidebar}
                        </div>
                      </form>
                    </div>
                  )}
                  <div
                    className={
                      mainOnly || drawerLayout
                        ? "col-12"
                        : "col-sm-9 ibpl-main-col"
                    }
                    role="main"
                  >
                    {main}
                  </div>
                </div>
              </div>
            </div>
          </div>
        </Navbar>
        {drawerLayout && (
          <FilterDrawer
            open={drawerOpen}
            onOpenChange={(open) => {
              setDrawerOpen(open);
              // Closed from inside (Escape, x, Show N): focus goes back to
              // the Filters button that opened it.
              if (!open)
                requestAnimationFrame(() =>
                  document
                    .querySelector<HTMLElement>(".chips-drawer-toggle")
                    ?.focus(),
                );
            }}
            sections={drawer.sections}
            openSections={openSections}
            onSections={setOpenSections}
            footer={drawer.footer}
            activeCount={drawer.activeCount}
            resettable={drawer.resettable}
            onClearAll={drawer.onClearAll}
            resultLabel={drawer.resultLabel}
          />
        )}
        {glossary && <Glossary onClose={() => setGlossary(false)} />}
      </FrameContext.Provider>
    </TouchTipsContext.Provider>
  );
}

/** The hidden view-mode radios (.view-mode-container); the navbar menu is the
 * visible control, as in Shiny. */
export function ViewRadios({
  name,
  label,
  views,
  view,
  tips,
  onView,
}: {
  name: string;
  label: string;
  views: string[];
  view: string;
  tips?: Record<string, string>;
  onView: (v: string) => void;
}) {
  return (
    <div className="view-mode-container">
      <div
        className="form-group shiny-input-radiogroup shiny-input-container shiny-input-container-inline"
        role="radiogroup"
        aria-label="View"
      >
        <label className="control-label">{label}</label>
        <div className="shiny-options-group">
          {views.map((v) => (
            <label key={v} className="radio-inline" data-tooltip={tips?.[v]}>
              <input
                type="radio"
                name={name}
                value={v}
                checked={view === v}
                onChange={() => onView(v)}
              />
              <span>{v}</span>
            </label>
          ))}
        </div>
      </div>
    </div>
  );
}

// ---- Static content ------------------------------------------------------
/** A view's explainer, example and legend blocks (shiny/Blocks), with the
 * glossary link wired to this frame. */
export function ViewContent({ blocks }: { blocks: Block[] }) {
  const { openGlossary } = useFrame();
  return <ViewBlocks blocks={blocks} onGlossary={openGlossary} />;
}

// ---- Sidebar controls ------------------------------------------------------
/** starter_context_filters_ui(): mode + value per side, stored as "mode:value". */
export function StarterRows({
  p,
  query,
  set,
}: {
  p: string;
  query: Query;
  set: (patch: Patch) => void;
}) {
  const row = (key: "own_starters" | "opp_starters", side: "off" | "def") => {
    const [mode = "", value = ""] = (query[key] ?? "").split(":");
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
            emptyLabel="ALL"
            choices={[
              { value: "gte", label: "At least (>=)" },
              { value: "lte", label: "At most (<=)" },
            ]}
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
            emptyLabel="--"
            choices={["0", "1", "2", "3", "4", "5"].map((n) => ({
              value: n,
              label: n,
            }))}
            value={[value]}
            onChange={(v) =>
              set({ [key]: mode || v[0] ? `${mode}:${v[0] ?? ""}` : null })
            }
          />
        </div>
      </div>
    );
  };
  return (
    <>
      {row("own_starters", "off")}
      {row("opp_starters", "def")}
    </>
  );
}

/** accordion_toggle_link() + game_context_filters_ui(). */
export function GameFiltersAccordion({
  p,
  league,
  query,
  set,
  teams,
  rounds,
  phases,
  oppRank,
  oppRankMetricDefault = "",
  bare = false,
}: {
  p: string;
  league: League;
  query: Query;
  set: (patch: Patch) => void;
  teams: Choice[];
  rounds: number[];
  phases: string[];
  oppRank: boolean;
  oppRankMetricDefault?: string;
  /** Inside the filters drawer: the groups as plain titled blocks (the
   * drawer section is the collapsible wrapper). */
  bare?: boolean;
}) {
  const euro = league !== "il";
  const { accordionOpen, setAccordionOpen } = useFrame();
  const [strengthOpen, setStrengthOpen] = useState(true);
  const typeKey = euro ? "phase" : "game_type";
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
    ? phases.map((ph) => ({ value: ph, label: PHASES[ph] ?? ph }))
    : catalogues.gameTypeChoices.filter((c) => c.value !== "");
  const rankMax = euro ? 20 : 12;
  const item = (
    title: ReactNode,
    open: boolean,
    toggle: () => void,
    id: string,
    children: ReactNode,
    value: string,
  ) =>
    bare ? (
      <div className="drawer-subgroup" data-value={value}>
        <div className="drawer-subtitle">{title}</div>
        {children}
      </div>
    ) : (
      <div className="accordion-item" data-value={value}>
        <div className="accordion-header">
          <button
            className={`accordion-button${open ? "" : " collapsed"}`}
            type="button"
            aria-expanded={open}
            aria-controls={id}
            onClick={toggle}
          >
            <div className="accordion-icon"></div>
            <div className="accordion-title">{title}</div>
          </button>
        </div>
        <div
          id={id}
          className={`accordion-collapse collapse${open ? " show" : ""}`}
        >
          <div className="accordion-body">{children}</div>
        </div>
      </div>
    );
  return (
    <>
      {!bare && (
        <div className="text-end mb-2">
          <a
            href="#"
            className="small text-muted fw-bold js-accordion-toggle-all"
            style={{ textDecoration: "none" }}
            onClick={(e) => {
              e.preventDefault();
              // app.js: any open -> close all, else open all.
              const any = accordionOpen || (oppRank && strengthOpen);
              setAccordionOpen(() => !any);
              setStrengthOpen(!any);
            }}
          >
            Collapse/Expand All
          </a>
        </div>
      )}
      <div className={bare ? "drawer-subgroups" : "accordion"}>
        {item(
          "Game Filters",
          accordionOpen,
          () => setAccordionOpen((o) => !o),
          `${p}-game-filters`,
          <>
            <Selectize
              id={`${p}_${typeKey}`}
              label={euro ? "Phase" : "Game type"}
              multiple
              placeholder={euro ? "All phases" : "All game types"}
              choices={typeChoices}
              value={list(query[typeKey] ?? "")}
              onChange={(v) => set({ [typeKey]: v.join(",") })}
            />
            <Selectize
              id={`${p}_opponents`}
              label="Opponents"
              multiple
              placeholder="All opponents"
              choices={teams}
              value={list(query.opponents ?? "")}
              onChange={(v) => set({ opponents: v.join(",") })}
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
              value={[query.home_away ?? ""]}
              onChange={(v) => set({ home_away: v[0] ?? null })}
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
              value={[query.outcome ?? ""]}
              onChange={(v) => set({ outcome: v[0] ?? null })}
            />
            <hr />
            <div className="row">
              <div className="col-sm-6">
                <Selectize
                  id={`${p}_gn_min`}
                  label={
                    euro ? "From Round" : tt("From Game Number (GN)", "gn")
                  }
                  placeholder="Any"
                  choices={roundChoices}
                  value={[query.min_gn ?? ""]}
                  onChange={(v) => set({ min_gn: v[0] ?? null })}
                />
              </div>
              <div className="col-sm-6">
                <Selectize
                  id={`${p}_gn_max`}
                  label={euro ? "To Round" : tt("To Game Number (GN)", "gn")}
                  placeholder="Any"
                  choices={roundChoices}
                  value={[query.max_gn ?? ""]}
                  onChange={(v) => set({ max_gn: v[0] ?? null })}
                />
              </div>
            </div>
            <Selectize
              id={`${p}_last_n`}
              label={tt("Last N Team Games", "last_n")}
              placeholder="Any"
              choices={lastChoices}
              value={[query.last_n ?? ""]}
              onChange={(v) => set({ last_n: v[0] ?? null })}
            />
          </>,
          "Game Filters",
        )}
        {oppRank &&
          item(
            tt("Opponent Strength", "opp_strength"),
            strengthOpen,
            () => setStrengthOpen((o) => !o),
            `${p}-opp-strength`,
            <>
              <Selectize
                id={`${p}_opp_rank_side`}
                label="Top / Bottom"
                formControl={false}
                placeholder="Off"
                emptyLabel="Off"
                choices={[
                  { value: "top", label: "Top" },
                  { value: "bottom", label: "Bottom" },
                ]}
                value={[query.opp_rank_side ?? ""]}
                onChange={(v) => set({ opp_rank_side: v[0] ?? null })}
              />
              <Selectize
                id={`${p}_opp_rank_n`}
                label="Rank N"
                formControl={false}
                placeholder="--"
                emptyLabel="--"
                choices={Array.from({ length: rankMax }, (_, i) => ({
                  value: String(i + 1),
                  label: String(i + 1),
                }))}
                value={[query.opp_rank_n ?? ""]}
                onChange={(v) => set({ opp_rank_n: v[0] ?? null })}
              />
              <Selectize
                id={`${p}_opp_rank_metric`}
                label="Metric"
                formControl={false}
                placeholder="--"
                emptyLabel="--"
                choices={[
                  { value: "off", label: "Offense" },
                  { value: "def", label: "Defense" },
                  { value: "net", label: "Net rating" },
                ]}
                value={[query.opp_rank_metric || oppRankMetricDefault]}
                onChange={(v) => set({ opp_rank_metric: v[0] ?? null })}
              />
            </>,
            "Opponent Strength",
          )}
      </div>
    </>
  );
}

// ---- Chips ---------------------------------------------------------------
/** make_chip(): the label, a pencil when it can reveal its control, the x. */
export function Chip({
  label,
  focus,
  onClear,
  className = "chip-game",
}: {
  label: string;
  focus?: string;
  onClear: () => void;
  className?: string;
}) {
  const { reveal } = useFrame();
  return (
    <span
      className={`filter-chip ${className}${focus ? " chip-focusable" : ""}`}
      data-chip-focus={focus}
      tabIndex={focus ? 0 : undefined}
      role={focus ? "button" : undefined}
      onClick={(e) => {
        if (focus && !(e.target as HTMLElement).closest(".chip-x"))
          reveal(focus);
      }}
      onKeyDown={(e) => {
        if (
          focus &&
          (e.key === "Enter" || e.key === " ") &&
          !(e.target as HTMLElement).closest(".chip-x")
        ) {
          e.preventDefault();
          reveal(focus);
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

/** build_filter_chips() for the game context, in its order. `datesAlways`:
 * the Israeli "gl", "ld" and "tr" tabs chip a set date range even at the
 * season bounds; the others only when it differs. */
export function contextChips({
  p,
  league,
  query,
  set,
  teams,
  datesAlways,
  teamKey,
  teamFocus,
  datesFocus,
}: {
  p: string;
  league: League;
  query: Query;
  set: (patch: Patch) => void;
  teams: Choice[];
  datesAlways: boolean;
  teamKey: "team" | "teams";
  teamFocus: string;
  datesFocus: string;
}): ReactNode[] {
  const euro = league !== "il";
  const name = (id: string) => teams.find((t) => t.value === id)?.label ?? id;
  const b = seasonBounds(league, query.season);
  const start = query.start || b.start,
    end = query.end || b.end;
  const chips: ReactNode[] = [];
  if (datesAlways || start !== b.start || end !== b.end)
    chips.push(
      <Chip
        key="dates"
        label={`${chipDate(start)} - ${chipDate(end)}`}
        focus={datesFocus}
        onClear={() => set({ start: null, end: null })}
      />,
    );
  const typeKey = euro ? "phase" : "game_type";
  const types = list(query[typeKey] ?? "");
  if (types.length)
    chips.push(
      <Chip
        key="type"
        focus={`${p}_${typeKey}`}
        onClear={() => set({ [typeKey]: null })}
        label={types
          .map((t) => (euro ? (PHASES[t] ?? t) : (GAME_TYPES[t] ?? t)))
          .join(", ")}
      />,
    );
  const picked = list(query[teamKey] ?? "");
  if (picked.length)
    chips.push(
      <Chip
        key="teams"
        focus={teamFocus}
        onClear={() => set({ [teamKey]: null })}
        label={picked.length === 1 ? name(picked[0]) : `${picked.length} teams`}
      />,
    );
  const opps = list(query.opponents ?? "");
  if (opps.length)
    chips.push(
      <Chip
        key="opps"
        focus={`${p}_opponents`}
        onClear={() => set({ opponents: null })}
        label={
          opps.length === 1 ? `vs ${name(opps[0])}` : `vs ${opps.length} opps`
        }
      />,
    );
  if (query.home_away)
    chips.push(
      <Chip
        key="ha"
        focus={`${p}_home_away`}
        label={query.home_away === "home" ? "Home" : "Away"}
        onClear={() => set({ home_away: null })}
      />,
    );
  if (query.outcome)
    chips.push(
      <Chip
        key="out"
        focus={`${p}_outcome`}
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
        label={`Last ${query.last_n} games`}
        onClear={() => set({ last_n: null })}
      />,
    );
  if (query.opp_rank_side) {
    let label = `vs ${query.opp_rank_side}`;
    if (query.opp_rank_n) label += ` ${query.opp_rank_n}`;
    if (query.opp_rank_metric) label += ` ${query.opp_rank_metric}`;
    chips.push(
      <Chip
        key="rank"
        focus={`${p}_opp_rank_side`}
        label={label}
        onClear={() =>
          set({ opp_rank_side: null, opp_rank_n: null, opp_rank_metric: null })
        }
      />,
    );
  }
  const sp = (["own_starters", "opp_starters"] as const)
    .map((k) => {
      const [mode = "", value = ""] = (query[k] ?? "").split(":");
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
        onClear={() => set({ own_starters: null, opp_starters: null })}
      />,
    );
  return chips;
}

type StatCol = { label: string; col: string };
/** stat_filter_chips_ui(): the active stat chips and the "+ Stat filter"
 * popover (bslib popover: Column, Operator, Value, Add). `groups` renders the
 * column menu's optgroups (stat_filter_choices()). */
export function StatFilters({
  p,
  cols,
  labels,
  groups,
  filters,
  onChange,
  hint,
  extraNote,
}: {
  p: string;
  cols: StatCol[];
  /** Keep labels for active filters whose column is hidden in this mode. */
  labels?: StatCol[];
  groups?: { label: string; cols: string[] }[];
  filters: StatFilter[];
  onChange: (f: StatFilter[]) => void;
  hint?: string;
  extraNote?: ReactNode;
}) {
  const [open, setOpen] = useState(false);
  const [col, setCol] = useState("");
  const [op, setOp] = useState<"ge" | "le">("ge");
  const [value, setValue] = useState("");
  const all = useMemo(() => cols, [cols]);
  const label = (c: string) =>
    (labels ?? all).find((x) => x.col === c)?.label ?? c;
  const add = () => {
    const picked = all.find((x) => x.label === col);
    const v = Number(value);
    if (!picked || value.trim() === "" || !Number.isFinite(v)) return;
    onChange([...filters, { col: picked.col, op, value: v }]);
    setCol("");
    setOp("ge");
    setValue("");
  };
  // Popper keeps the popover inside the viewport; the arrow stays on the chip.
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
  const choices = groups
    ? groups.flatMap((g) =>
        all
          .filter((c) => g.cols.includes(c.col))
          .map((c) => ({ value: c.label, label: c.label, group: g.label })),
      )
    : all.map((c) => ({ value: c.label, label: c.label }));
  const pct =
    hint ??
    (all.some((c) => c.label.includes("%"))
      ? "Percent columns: enter as 0-100."
      : undefined);
  return (
    <>
      {filters.map((f, i) => (
        <span key={`${f.col}-${i}`} className="filter-chip chip-stat">
          {`${label(f.col)} ${f.op === "ge" ? ">=" : "<="} ${f.value.toLocaleString("en-US", { maximumFractionDigits: 7 })}`}{" "}
          <a
            href="#"
            style={{ marginLeft: 4, color: "inherit" }}
            aria-label={`Remove ${label(f.col)} filter`}
            onClick={(e) => {
              e.preventDefault();
              onChange(filters.filter((_, j) => j !== i));
            }}
          >
            ×
          </a>
        </span>
      ))}
      <span style={{ position: "relative", display: "inline-block" }}>
        <span
          className="filter-chip filter-chip-add"
          id={`${p}_stat_filter_add_btn`}
          role="button"
          tabIndex={0}
          aria-pressed={open}
          aria-expanded={open}
          style={{ cursor: "pointer" }}
          onClick={() => setOpen((o) => !o)}
          onKeyDown={(e) => {
            if (e.key === "Enter" || e.key === " ") {
              e.preventDefault();
              setOpen((o) => !o);
            }
          }}
        >
          <i className="bi bi-plus"></i> Stat filter
        </span>
        {open && (
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
            <h3 className="popover-header">Add stat filter</h3>
            <div className="popover-body">
              <div className={`${p}-stat-popover`} style={{ minWidth: 220 }}>
                <Selectize
                  id={`${p}_stat_filter_col`}
                  label="Column"
                  formControl={false}
                  placeholder="Choose..."
                  style={{ width: "100%" }}
                  choices={choices}
                  value={[col]}
                  onChange={(v) => setCol(v[0] ?? "")}
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
                    {(["ge", "le"] as const).map((o) => (
                      <label key={o} className="radio-inline">
                        <input
                          type="radio"
                          name={`${p}_stat_filter_op`}
                          value={o}
                          checked={op === o}
                          onChange={() => setOp(o)}
                        />
                        <span>{o === "ge" ? ">=" : "<="}</span>
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
                    value={value}
                    onChange={(e) => setValue(e.target.value)}
                    onKeyDown={(e) => e.key === "Enter" && add()}
                  />
                </div>
                {pct && <div className="small text-muted mb-2">{pct}</div>}
                <button
                  className="btn btn-default action-button btn-sm btn-primary w-100"
                  type="button"
                  onClick={add}
                >
                  Add
                </button>
              </div>
              <button
                type="button"
                aria-label="Close"
                className="btn-close"
                onClick={() => setOpen(false)}
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
      {extraNote}
    </>
  );
}

/** filter_chips_row(): the Filters toggle, the chips (season first, "Clear
 * all" when anything is active), and optional controls on the right. */
export function ChipsRow({
  p,
  seasonChip,
  chips,
  extra,
  active,
  onClearAll,
  controls,
  activeCount = 0,
}: {
  p: string;
  seasonChip: string;
  chips: ReactNode[];
  extra?: ReactNode;
  active: boolean;
  onClearAll: () => void;
  controls?: ReactNode;
  /** Drawer layout: the number shown on the Filters button. */
  activeCount?: number;
}) {
  const { filtersCollapsed, toggleFilters, layout, drawerOpen, setDrawerOpen } =
    useFrame();
  const drawer = layout === "drawer";
  return (
    <div className="chips-row">
      {drawer ? (
        <button
          type="button"
          className="chips-filters-toggle chips-drawer-toggle"
          aria-expanded={drawerOpen}
          onClick={() => setDrawerOpen(!drawerOpen)}
        >
          <i className="bi bi-sliders" aria-hidden="true"></i>
          <span className="chips-filters-toggle-label">
            Filters{activeCount ? ` · ${activeCount}` : ""}
          </span>
        </button>
      ) : (
        <button
          type="button"
          className="chips-filters-toggle js-filters-toggle"
          aria-expanded={!filtersCollapsed}
          aria-label={
            filtersCollapsed ? "Show the filter panel" : "Hide the filter panel"
          }
          onClick={toggleFilters}
        >
          <i className="bi bi-sliders" aria-hidden="true"></i>
          <span className="chips-filters-toggle-label">Filters</span>
        </button>
      )}
      <div className="chips-row-chips">
        <div id={`${p}_filter_chips`} aria-live="polite">
          <div className="filter-chips">
            <span className="filter-chip chip-season">{seasonChip}</span>
            {chips}
            {extra}
            {active && (
              <button
                className="chip-clear-all"
                type="button"
                onClick={onClearAll}
              >
                Clear all
              </button>
            )}
          </div>
        </div>
      </div>
      {controls && <div className="chips-row-controls">{controls}</div>}
    </div>
  );
}

/** The season chip text: "26-27", or "EuroLeague 25-26". */
export function seasonChipLabel(league: League, season: string) {
  return league === "il"
    ? seasonLabel(season)
    : `${ui.euroCompetitionLabels[league === "euroleague" ? "E" : "U"]} ${seasonLabel(season, false)}`;
}

/** Output area states: the error with a retry, or the busy dimming. */
export function OutputArea({
  id,
  loading,
  error,
  retry,
  children,
}: {
  id: string;
  loading: boolean;
  error: string;
  retry: () => void;
  children: ReactNode;
}) {
  return (
    <div
      id={id}
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
            onClick={retry}
          >
            Retry
          </button>
        </div>
      ) : (
        children
      )}
    </div>
  );
}
