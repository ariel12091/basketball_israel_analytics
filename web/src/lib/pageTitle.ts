// Page names and league names for <title> and each page's <h1>. A plain
// module (not "use client") so server pages' generateMetadata can read it.
import type { Metadata } from "next";
import ui from "@/data/shinyUi.json";
import type { League } from "@/rules/gameLogs";

/** Each route's name, as its navbar tab says it. */
export const PAGE_LABEL = {
  home: "Home",
  onoff: "On/Off Impact",
  lineups: "Lineup Data",
  "team-ratings": "Team Ratings",
  "game-logs": "Game Logs",
  "player-stats": "Player Stats",
  compare: "Compare",
  gameflow: "Gameflow",
} as const;
export type PagePath = keyof typeof PAGE_LABEL;

/** Shiny's league select values (also the stored league choice). */
export const LEAGUE_VALUE: Record<League, string> = {
  il: "il",
  euroleague: "E",
  eurocup: "U",
};

/** The league's full name from Shiny's league select. */
export function leagueName(league: League): string {
  return (
    ui.leagueChoices.find((c) => c.value === LEAGUE_VALUE[league])?.label ??
    league
  );
}

/** "On/Off Impact · Israeli League", the page part of every title. */
export function pageHeading(path: PagePath, league: League): string {
  return `${PAGE_LABEL[path]} · ${leagueName(league)}`;
}

/** A route's generateMetadata: the root layout's template adds the site. */
export function pageMetadata(path: PagePath) {
  return async ({
    params,
  }: {
    params: Promise<{ league: string }>;
  }): Promise<Metadata> => {
    const { league } = await params;
    if (!(league in LEAGUE_VALUE)) return {};
    return { title: pageHeading(path, league as League) };
  };
}
