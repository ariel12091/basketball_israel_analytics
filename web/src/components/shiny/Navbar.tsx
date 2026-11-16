"use client";
// The site's top bar (navbar redesign, user 2026-11-16; mock-up
// https://claude.ai/artifact/CoQ1bLn23aSxFVegvRzmhc). One markup for every
// width; web.css arranges it:
// - wide screens: one row of page pills, then the settings (league switch,
//   season menu) set apart by dividers and small labels, then the update
//   date, Glossary and the theme switch;
// - narrow screens: the bar keeps the logo, league and season menus, Glossary
//   and theme; the page pills become a tab bar fixed to the bottom.
// The views of each page are tabs on the page itself (ViewTabs), so the bar
// has no view menus.
import { useState } from "react";
import * as Popover from "@radix-ui/react-popover";
import Link from "next/link";
import type { League } from "@/rules/gameLogs";
import {
  LEAGUE_VALUE,
  leagueName,
  pageHeading,
  type PagePath,
} from "@/lib/pageTitle";
import ThemeToggle from "./ThemeToggle";

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
  /** The bar's short name; the page heading keeps the full one. */
  label: string;
  icon: string;
  path: PagePath;
  ilOnly?: boolean;
};
// Tab order: app.R build_ui(); icons: the ui_tab*.R titles.
const TABS: Tab[] = [
  { value: "home", label: "Home", icon: "bi-house-fill", path: "home" },
  {
    value: "onoff",
    euroValue: "euro",
    label: "On/Off",
    icon: "bi-toggles",
    path: "onoff",
  },
  {
    value: "lineup_data",
    euroValue: "euro_lineups",
    label: "Lineups",
    icon: "bi-people-fill",
    path: "lineups",
  },
  {
    value: "team_ratings",
    euroValue: "euro_team",
    label: "Teams",
    icon: "bi-trophy-fill",
    path: "team-ratings",
  },
  {
    value: "game_logs",
    euroValue: "euro_game_logs",
    label: "Games",
    icon: "bi-calendar-event",
    path: "game-logs",
  },
  {
    value: "traditional_stats",
    label: "Players",
    icon: "bi-bar-chart-line",
    path: "player-stats",
  },
  {
    value: "compare",
    label: "Compare",
    icon: "bi-arrow-left-right",
    path: "compare",
    ilOnly: true,
  },
];

const LEAGUES: League[] = ["il", "euroleague", "eurocup"];
/** The league switch's words; the menu and the page title use full names. */
const LEAGUE_SHORT: Record<League, string> = {
  il: "Israeli",
  euroleague: "EuroLeague",
  eurocup: "EuroCup",
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
/** "Nov 14, 22:27" from "2026-11-14 22:27:46" (the time when it has one). */
function lastUpdatedLong(txt: string) {
  const t = /\d{4}-\d{2}-\d{2}[ T](\d{2}:\d{2})/.exec(txt);
  return `${lastUpdatedShort(txt)}${t ? `, ${t[1]}` : ""}`;
}

function rememberLeague(league: League) {
  try {
    window.localStorage.setItem("ibpl_league_select", LEAGUE_VALUE[league]);
  } catch {}
}

export default function Navbar({
  league,
  tab,
  season,
  seasons,
  onSeason,
  onGlossary,
  updated,
  page,
  children,
}: {
  league: League;
  tab: TabId;
  season: string;
  seasons: string[];
  onSeason: (s: string) => void;
  onGlossary: () => void;
  updated: string | null;
  /** The route, when it is not the tab's own (Gameflow sits under Game Logs). */
  page?: PagePath;
  children: React.ReactNode;
}) {
  const euro = league !== "il";
  const currentTab = TABS.find((t) => t.value === tab);
  // app.js applyValue(): a league-only page falls back to Home when switching
  // away from its league. EuroLeague and EuroCup share a season convention,
  // so a switch between them keeps the season.
  const leagueHref = (l: League) => {
    const path =
      l !== "il" && currentTab?.ilOnly ? "home" : (currentTab?.path ?? "home");
    return `/${l}/${path}${euro && l !== "il" ? `?season=${season}` : ""}`;
  };
  const stamp = updated
    ? `Data updated ${lastUpdatedLong(updated)}`
    : "Update time unavailable";
  const short = updated ? lastUpdatedShort(updated) : "n/a";
  const seasonMenu = (
    <NavMenu
      id={euro ? "euro_game_year" : "game_year"}
      name="Season"
      title={`${LEAGUE_SHORT[league]} season`}
      current={seasonLabel(season, !euro)}
      align="end"
      footer={stamp}
      items={seasons.map((s) => ({
        key: s,
        label: seasonLabel(s, !euro),
        active: s === season,
        onSelect: () => s !== season && onSeason(s),
      }))}
    />
  );
  return (
    <>
      <header className="ibpl-topbar">
        <div className="ibpl-topbar-row">
          <Link
            className="ibpl-brand"
            href={`/${league}/home?season=${season}`}
            prefetch={false}
            aria-label="IBPL Analytics home"
          >
            <i className="bi bi-activity" aria-hidden="true"></i>
            <span className="ibpl-brand-name">IBPL Analytics</span>
          </Link>
          <nav className="ibpl-pages" id="ibpl_pages" aria-label="Pages">
            <ul>
              {TABS.filter((t) => !(euro && t.ilOnly)).map((t) => {
                const here = t.value === tab;
                return (
                  <li key={t.value}>
                    <Link
                      href={`/${league}/${t.path}?season=${season}`}
                      prefetch={false}
                      data-value={euro ? (t.euroValue ?? t.value) : t.value}
                      className={`ibpl-page${here ? " active" : ""}`}
                      aria-current={here ? "page" : undefined}
                      // The current page's link keeps its filters: a click
                      // on it stays put, as in Shiny.
                      onClick={here ? (e) => e.preventDefault() : undefined}
                    >
                      <i className={`bi ${t.icon}`} aria-hidden="true"></i>
                      <span className="ibpl-page-label">{t.label}</span>
                    </Link>
                  </li>
                );
              })}
            </ul>
          </nav>
          <div className="ibpl-settings">
            <span className="ibpl-sep" aria-hidden="true"></span>
            <div className="ibpl-setting">
              <span className="ibpl-setting-label" id="ibpl_league_label">
                League
              </span>
              {/* Wide screens: every league one click away. */}
              <div
                className="ibpl-league-switch"
                role="group"
                aria-labelledby="ibpl_league_label"
              >
                {LEAGUES.map((l) => (
                  <Link
                    key={l}
                    href={leagueHref(l)}
                    prefetch={false}
                    className={l === league ? "active" : undefined}
                    aria-current={l === league ? "page" : undefined}
                    title={leagueName(l)}
                    onClick={() => rememberLeague(l)}
                  >
                    {LEAGUE_SHORT[l]}
                  </Link>
                ))}
              </div>
              {/* Narrow screens: the same choice as a menu. */}
              <NavMenu
                id="league"
                name="League"
                className="ibpl-league-menu"
                current={LEAGUE_SHORT[league]}
                items={LEAGUES.map((l) => ({
                  key: l,
                  label: leagueName(l),
                  active: l === league,
                  href: leagueHref(l),
                  onSelect: () => rememberLeague(l),
                }))}
              />
            </div>
            <div className="ibpl-setting">
              <span className="ibpl-setting-label" aria-hidden="true">
                Season
              </span>
              {seasonMenu}
            </div>
            <span className="ibpl-sep" aria-hidden="true"></span>
            <span className="ibpl-updated" id="ibpl_updated" title={stamp}>
              <span className="ibpl-updated-dot" aria-hidden="true"></span>
              <span className="visually-hidden">Data updated </span>
              {short}
            </span>
            <button
              className="ibpl-icon-btn"
              id="open_glossary"
              type="button"
              title="Glossary"
              aria-label="Glossary"
              onClick={onGlossary}
            >
              <i className="bi bi-book" aria-hidden="true"></i>
            </button>
            <ThemeToggle id="theme_toggle" className="ibpl-icon-btn" />
          </div>
        </div>
      </header>
      <div className="container-fluid">
        {/* One h1 per page for screen readers and outlines; the bar's
            active page already says it on screen. */}
        <h1 className="ibpl-sr-only">
          {pageHeading(page ?? currentTab?.path ?? "home", league)}
        </h1>
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
/** A pill showing the current value with a caret, opening a small list of
 * the choices. It is navigation, not an ARIA menu (modern-web-guidance
 * "navbar"): links and buttons in the Tab order, no aria-haspopup or menu
 * roles. Radix Popover gives the light dismiss, Escape, focus return and
 * edge-aware placement that the hand-written listeners used to. */
function NavMenu({
  id,
  name,
  title,
  footer,
  current,
  items,
  className,
  align = "start",
}: {
  id: string;
  name: string;
  /** A heading inside the menu. */
  title?: string;
  /** A quiet line under the choices. */
  footer?: string;
  current: string;
  items: NavMenuItem[];
  className?: string;
  /** Which edge of the pill the menu lines up with. */
  align?: "start" | "end";
}) {
  const [open, setOpen] = useState(false);
  const choose = (item: NavMenuItem) => {
    setOpen(false);
    item.onSelect?.();
  };
  return (
    <div className={`ibpl-menu${className ? ` ${className}` : ""}`}>
      <Popover.Root open={open} onOpenChange={setOpen}>
        <Popover.Trigger className="ibpl-menu-btn" aria-haspopup={undefined}>
          <span className="visually-hidden">{name}: </span>
          {current}
          <i className="bi bi-chevron-down" aria-hidden="true"></i>
        </Popover.Trigger>
        <Popover.Portal>
          <Popover.Content
            className="ibpl-menu-list"
            role={undefined}
            aria-label={name}
            align={align}
            sideOffset={6}
            collisionPadding={8}
          >
            <div id={`${id}_menu`}>
              {title && <div className="ibpl-menu-title">{title}</div>}
              <ul>
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
                        {item.active && (
                          <i className="bi bi-check2" aria-hidden="true"></i>
                        )}
                      </Link>
                    ) : (
                      <button
                        type="button"
                        className={item.active ? "active" : undefined}
                        aria-pressed={item.active}
                        onClick={() => choose(item)}
                      >
                        {item.label}
                        {item.active && (
                          <i className="bi bi-check2" aria-hidden="true"></i>
                        )}
                      </button>
                    )}
                  </li>
                ))}
              </ul>
              {footer && <p className="ibpl-menu-footer">{footer}</p>}
            </div>
          </Popover.Content>
        </Popover.Portal>
      </Popover.Root>
    </div>
  );
}
