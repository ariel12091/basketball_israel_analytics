"use client";
// Shiny's navbarPage header (app.R build_ui) with the tab view-mode menus that
// app.js builds from its CFG list, and the fixed right cluster: league select,
// season select, Glossary button, last-updated stamp. Migrated tabs are links
// (keeping the season); the others keep their place and look but are inert.
import { useEffect, useRef, useState } from "react";
import { useRouter } from "next/navigation";
import Link from "next/link";
import ui from "@/data/shinyUi.json";
import type { League } from "@/rules/gameLogs";

export type TabId =
  | "home"
  | "onoff"
  | "lineup_data"
  | "team_ratings"
  | "game_logs"
  | "traditional_stats"
  | "compare";
type Tab = {
  value: TabId;
  euroValue?: string;
  label: string;
  icon: string;
  modes?: string[];
  def?: string;
  ilOnly?: boolean;
  path?: string;
};
// Tab order: app.R build_ui(); icons: the ui_tab*.R titles; modes: app.js CFG.
const TABS: Tab[] = [
  { value: "home", label: "Home", icon: "bi-house-fill", path: "home" },
  {
    value: "onoff",
    euroValue: "euro",
    label: "On/Off Impact",
    icon: "bi-toggles",
    modes: ["Summary", "Four Factors", "Shot Profile"],
    def: "Summary",
    path: "onoff",
  },
  {
    value: "lineup_data",
    euroValue: "euro_lineups",
    label: "Lineup Data",
    icon: "bi-people-fill",
    modes: ["Summary", "Four Factors"],
    def: "Summary",
    path: "lineups",
  },
  {
    value: "team_ratings",
    euroValue: "euro_team",
    label: "Team Ratings",
    icon: "bi-trophy-fill",
    modes: ["Summary", "Four Factors", "Shot Profile", "Traditional"],
    def: "Summary",
    path: "team-ratings",
  },
  {
    value: "game_logs",
    euroValue: "euro_game_logs",
    label: "Game Logs",
    icon: "bi-calendar-event",
    modes: ["Summary", "Four Factors"],
    def: "Summary",
    path: "game-logs",
  },
  {
    value: "traditional_stats",
    path: "player-stats",
    label: "Player Stats",
    icon: "bi-bar-chart-line",
    modes: ["Totals", "Per Game", "Per 60 Possessions", "Per 30 Minutes"],
    def: "Per Game",
  },
  {
    value: "compare",
    label: "Compare",
    icon: "bi-arrow-left-right",
    modes: ["Teams", "Lineups", "Players"],
    def: "Teams",
    ilOnly: true,
    path: "compare",
  },
];
// app.js CFG for the EuroLeague tabs: On/Off and Team Ratings have two modes.
const EURO_MODES: Record<string, string[]> = {
  euro: ["Summary", "Four Factors"],
  euro_team: ["Summary", "Four Factors"],
};
const VIEW_PARAM: Record<string, string> = {
  Totals: "Totals",
  "Per Game": "Per Game",
  "Per 60 Possessions": "Per 60 Possessions",
  "Per 30 Minutes": "Per 30 Minutes",
  Traditional: "traditional",
  Summary: "summary",
  "Four Factors": "four_factors",
  "Shot Profile": "shot_profile",
};

const LEAGUE_VALUE: Record<League, string> = {
  il: "il",
  euroleague: "E",
  eurocup: "U",
};
// Israel first, each under its full name.
const LEAGUE_TOGGLE = (["il", "euroleague", "eurocup"] as const).map(
  (league) => ({
    league: league as League,
    name:
      ui.leagueChoices.find((c) => c.value === LEAGUE_VALUE[league])?.label ??
      league,
  }),
);

export function seasonLabel(year: string | number, endingYear = true) {
  const y = Number(year) + (endingYear ? 0 : 1);
  const two = (n: number) => String(((n % 100) + 100) % 100).padStart(2, "0");
  return `${two(y - 1)}-${two(y)}`;
}
// last_updated_short(): "Oct 20" from "2026-10-20 21:41:48".
const MONTH_ABB = [
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
export function lastUpdatedShort(txt: string) {
  const m = /(\d{4})-(\d{2})-(\d{2})/.exec(txt);
  return m ? `${MONTH_ABB[Number(m[2]) - 1]} ${Number(m[3])}` : "n/a";
}

export default function Navbar({
  league,
  tab,
  season,
  seasons,
  views,
  view,
  onView,
  onSeason,
  onGlossary,
  updated,
  collapsedNav,
  children,
}: {
  league: League;
  tab: TabId;
  season: string;
  seasons: string[];
  views: string[];
  view: string;
  onView: (v: string) => void;
  onSeason: (s: string) => void;
  onGlossary: () => void;
  updated: string | null;
  collapsedNav: boolean;
  children: React.ReactNode;
}) {
  const router = useRouter();
  const euro = league !== "il";
  const [collapsed, setCollapsed] = useState(true);
  const tabsRef = useRef<HTMLUListElement>(null);
  const clusterRef = useRef<HTMLDivElement>(null);

  // app.js: reserve the part of the fixed cluster that overhangs the tab row.
  useEffect(() => {
    const sync = () => {
      const c = clusterRef.current?.getBoundingClientRect();
      const u = tabsRef.current?.getBoundingClientRect();
      if (!c?.width || !u?.width) return;
      document.documentElement.style.setProperty(
        "--navbar-cluster-w",
        `${Math.max(0, Math.ceil(u.right - c.left) + 8)}px`,
      );
    };
    sync();
    const ro = new ResizeObserver(sync);
    if (clusterRef.current) ro.observe(clusterRef.current);
    window.addEventListener("resize", sync);
    return () => {
      ro.disconnect();
      window.removeEventListener("resize", sync);
    };
  }, [updated, collapsedNav]);

  const stamp = updated
    ? `Last updated: ${updated}`
    : "Last updated: unavailable";
  // mobile.js relocates the fixed cluster into the collapsed menu.
  const cluster = (
    <div
      id="navbar_right_cluster"
      ref={clusterRef}
      style={{
        position: "fixed",
        right: 10,
        top: 8,
        fontSize: "0.8rem",
        color: "var(--ibpl-text-muted)",
        zIndex: 9999,
        display: "flex",
        alignItems: "center",
        gap: 6,
        maxWidth: "calc(100vw - 20px)",
        whiteSpace: "nowrap",
      }}
    >
      {/* "Showing <league> . <season>": both plain-text menus. */}
      <div className="navbar-showing">
        <span className="navbar-showing-label">Showing</span>
        <NavMenu
          id="league"
          name="League"
          current={LEAGUE_TOGGLE.find((l) => l.league === league)!.name}
          items={LEAGUE_TOGGLE.map((l) => {
            const currentTab = TABS.find((t) => t.value === tab);
            // app.js applyValue(): a league-only tab must fall back to Home
            // when switching away from its league. Euro has no Compare route.
            const path =
              l.league !== "il" && currentTab?.ilOnly
                ? "home"
                : (currentTab?.path ?? "home");
            return {
              key: l.league,
              label: l.name,
              active: l.league === league,
              // EuroLeague and EuroCup share a season convention, so a
              // switch between them keeps the season.
              href: `/${l.league}/${path}${euro && l.league !== "il" ? `?season=${season}` : ""}`,
              onSelect: () => {
                try {
                  window.localStorage.setItem(
                    "ibpl_league_select",
                    LEAGUE_VALUE[l.league],
                  );
                } catch {}
              },
            };
          })}
        />
        <span className="navbar-showing-sep" aria-hidden="true">
          &middot;
        </span>
        <span
          className={`navbar-season-menu ${euro ? "league-nav-el" : "league-nav-il"}`}
        >
          <NavMenu
            id={euro ? "euro_game_year" : "game_year"}
            name="Season"
            current={seasonLabel(season, !euro)}
            items={seasons.map((s) => ({
              key: s,
              label: seasonLabel(s, !euro),
              active: s === season,
              onSelect: () => s !== season && onSeason(s),
            }))}
          />
        </span>
      </div>
      <button
        className="btn btn-default action-button btn btn-sm btn-outline-secondary nav-help-btn"
        id="open_glossary"
        type="button"
        onClick={onGlossary}
      >
        <span>
          <i className="bi bi-book" aria-hidden="true"></i> Glossary
        </span>
      </button>
      <span
        style={{
          display: "inline-flex",
          alignItems: "center",
          gap: 4,
          minWidth: 0,
        }}
      >
        <span
          className="nav-updated-dot"
          style={{
            width: 6,
            height: 6,
            background: "var(--ibpl-pos)",
            borderRadius: "50%",
            display: "inline-block",
          }}
        ></span>
        <span
          style={{
            display: "inline-block",
            maxWidth: 210,
            overflow: "hidden",
            textOverflow: "ellipsis",
            whiteSpace: "nowrap",
          }}
        >
          <span id="last_updated">
            <span title={stamp}>
              {updated ? lastUpdatedShort(updated) : "n/a"}
            </span>
          </span>
        </span>
      </span>
    </div>
  );
  return (
    <>
      <nav
        className="navbar navbar-default navbar-static-top"
        role="navigation"
        data-bs-theme="light"
      >
        <div className="container-fluid">
          <div className="navbar-header">
            <button
              type="button"
              className={`navbar-toggle${collapsed ? " collapsed" : ""}`}
              aria-expanded={!collapsed}
              aria-controls="navbar-collapse-main"
              onClick={() => setCollapsed((c) => !c)}
            >
              <span className="sr-only visually-hidden">Toggle navigation</span>
              <span className="icon-bar"></span>
              <span className="icon-bar"></span>
              <span className="icon-bar"></span>
            </button>
            <span className="navbar-brand">
              <span>
                <i
                  className="bi bi-activity"
                  style={{ marginRight: 6 }}
                  aria-hidden="true"
                ></i>
                IBPL Analytics
              </span>
            </span>
          </div>
          <div
            className={`navbar-collapse collapse${collapsed ? "" : " show"}`}
            id="navbar-collapse-main"
          >
            <ul
              ref={tabsRef}
              className="nav navbar-nav nav-underline"
              id="main_tabs"
              role="tablist"
            >
              {TABS.filter((t) => !(euro && t.ilOnly)).map((t) => {
                const value = euro ? (t.euroValue ?? t.value) : t.value;
                const here = t.value === tab;
                const modes = euro ? (EURO_MODES[value] ?? t.modes) : t.modes;
                const current = here ? view : t.def;
                const href = t.path
                  ? `/${league}/${t.path}?season=${season}`
                  : undefined;
                const go = (mode?: string) => {
                  if (!href) return;
                  router.push(
                    mode && VIEW_PARAM[mode] && mode !== t.def
                      ? `${href}&view=${VIEW_PARAM[mode]}`
                      : href,
                  );
                };
                const linkProps = {
                  "data-value": value,
                  className: `nav-link${here ? " active" : ""}`,
                  "aria-selected": here,
                  role: "tab",
                };
                const inner = (
                  <>
                    <span>
                      <i className={`bi ${t.icon}`} aria-hidden="true"></i>{" "}
                      {t.label}
                    </span>
                    {modes && <span className="tab-mode-label">{current}</span>}
                  </>
                );
                return (
                  <li
                    key={t.value}
                    className={`nav-item${modes ? " tab-has-dropdown" : ""}`}
                    role="presentation"
                  >
                    {href && !here ? (
                      <Link href={href} {...linkProps}>
                        {inner}
                      </Link>
                    ) : (
                      <a
                        {...linkProps}
                        href={here ? undefined : "#"}
                        aria-disabled={here ? undefined : true}
                        title={
                          here ? undefined : "Not yet available in the new app"
                        }
                        tabIndex={here ? 0 : -1}
                        onClick={(e) => e.preventDefault()}
                      >
                        {inner}
                      </a>
                    )}
                    {modes && (
                      <div className="tab-hover-menu">
                        {modes.map((m) => {
                          const usable = here
                            ? views.includes(m)
                            : Boolean(href);
                          const choose = () => {
                            if (!usable) return;
                            if (here) onView(m);
                            else go(m);
                            setCollapsed(true);
                          };
                          return (
                            <div
                              key={m}
                              data-value={m}
                              className={`thm-item${m === current ? " active" : ""}`}
                              role="menuitemradio"
                              aria-checked={m === current}
                              aria-disabled={usable ? undefined : true}
                              tabIndex={usable ? 0 : -1}
                              onClick={(e) => {
                                e.preventDefault();
                                choose();
                                const menu = e.currentTarget.parentElement;
                                if (menu) {
                                  menu.style.display = "none";
                                  setTimeout(
                                    () => (menu.style.display = ""),
                                    50,
                                  );
                                }
                              }}
                              onKeyDown={(e) => {
                                if (e.key === "Enter" || e.key === " ") {
                                  e.preventDefault();
                                  choose();
                                }
                              }}
                            >
                              <span className="thm-check">
                                {m === current ? "✓" : ""}
                              </span>
                              {m}
                            </div>
                          );
                        })}
                      </div>
                    )}
                  </li>
                );
              })}
            </ul>
            {collapsedNav && cluster}
          </div>
        </div>
      </nav>
      {/* Shiny puts the fixed cluster first inside the body container, so
          bslib's ".navbar + div > .tab-content > .tab-pane" spacing applies. */}
      <div className="container-fluid">
        {!collapsedNav && cluster}
        {children}
      </div>
    </>
  );
}

type NavMenuItem = {
  key: string;
  label: string;
  active: boolean;
  /** A link when set, a button otherwise; onSelect runs either way. */
  href?: string;
  onSelect?: () => void;
};
/** A navbar control shown as plain text with a caret (the current value),
 * opening a small menu of the choices. */
function NavMenu({
  id,
  name,
  current,
  items,
}: {
  id: string;
  name: string;
  current: string;
  items: NavMenuItem[];
}) {
  const [open, setOpen] = useState(false);
  const root = useRef<HTMLDivElement>(null);
  const button = useRef<HTMLButtonElement>(null);
  useEffect(() => {
    if (!open) return;
    const away = (e: PointerEvent) => {
      if (!root.current?.contains(e.target as Node)) setOpen(false);
    };
    document.addEventListener("pointerdown", away);
    return () => document.removeEventListener("pointerdown", away);
  }, [open]);
  const choose = (item: NavMenuItem) => {
    setOpen(false);
    item.onSelect?.();
  };
  return (
    <div
      className="navbar-text-menu"
      ref={root}
      onKeyDown={(e) => {
        if (e.key === "Escape" && open) {
          setOpen(false);
          button.current?.focus();
        }
      }}
    >
      <button
        ref={button}
        type="button"
        className="navbar-text-menu-current"
        aria-haspopup="true"
        aria-expanded={open}
        aria-controls={`${id}_menu`}
        onClick={() => setOpen((v) => !v)}
      >
        <span className="visually-hidden">{name}: </span>
        {current}
      </button>
      {open && (
        <ul id={`${id}_menu`} className="navbar-text-menu-options">
          {items.map((item) => (
            <li key={item.key}>
              {item.href ? (
                <Link
                  prefetch={false}
                  href={item.href}
                  className={item.active ? "active" : undefined}
                  aria-current={item.active ? "page" : undefined}
                  onClick={() => choose(item)}
                >
                  {item.label}
                </Link>
              ) : (
                <button
                  type="button"
                  className={item.active ? "active" : undefined}
                  aria-pressed={item.active}
                  onClick={() => choose(item)}
                >
                  {item.label}
                </button>
              )}
            </li>
          ))}
        </ul>
      )}
    </div>
  );
}
