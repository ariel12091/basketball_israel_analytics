"use client";
// Shiny's navbarPage header (app.R build_ui) with the tab view-mode menus that
// app.js builds from its CFG list, and the fixed right cluster: league select,
// season select, Glossary button, last-updated stamp. Migrated tabs are links
// (keeping the season); the others keep their place and look but are inert.
import { useEffect, useRef, useState } from "react";
import { useRouter } from "next/navigation";
import Link from "next/link";
import Selectize from "./Selectize";
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
  { value: "home", label: "Home", icon: "bi-house-fill" },
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
const VALUE_LEAGUE: Record<string, League> = {
  il: "il",
  E: "euroleague",
  U: "eurocup",
};

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
      <div className="navbar-league-select">
        <Selectize
          id="league_select"
          formControl={false}
          choices={ui.leagueChoices}
          value={[LEAGUE_VALUE[league]]}
          onChange={(v) => {
            const next = VALUE_LEAGUE[v[0] ?? "il"] ?? "il";
            try {
              window.localStorage.setItem("ibpl_league_select", v[0] ?? "il");
            } catch {}
            const path = TABS.find((t) => t.value === tab)?.path ?? "game-logs";
            if (next !== league) router.push(`/${next}/${path}`);
          }}
        />
      </div>
      <div
        className={`navbar-season-select ${euro ? "league-nav-el" : "league-nav-il"}`}
      >
        <Selectize
          id={euro ? "euro_game_year" : "game_year"}
          formControl={false}
          choices={seasons.map((s) => ({
            value: s,
            label: seasonLabel(s, !euro),
          }))}
          value={[season]}
          onChange={(v) => v[0] && onSeason(v[0])}
        />
      </div>
      <button
        className="btn btn-default action-button btn btn-sm btn-outline-secondary nav-help-btn"
        id="open_glossary"
        type="button"
        onClick={onGlossary}
      >
        <span>
          <i className="bi bi-book"></i> Glossary
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
                <i className="bi bi-activity" style={{ marginRight: 6 }}></i>
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
                      <i className={`bi ${t.icon}`}></i> {t.label}
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
