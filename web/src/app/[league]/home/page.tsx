import { Suspense } from "react";
import { headers } from "next/headers";
import { notFound } from "next/navigation";
import HomeClient from "@/components/HomeClient";
import { homeAnswer } from "@/server/homeRepo";
import { homeTeams, homePath } from "@/lib/homeQuery";
import { defaultSeason, type SearchParams } from "@/lib/tabQuery";
import type { League } from "@/rules/gameLogs";
import ui from "@/data/shinyUi.json";
// Like Shiny's startup team: one automatic choice per season in this worker.
const automaticTeams = new Map(
  Object.entries(ui.ilTeams).map(([season, teams]) => [
    season,
    String(teams[Math.floor(Math.random() * teams.length)]?.id ?? ""),
  ]),
);
export default async function HomePage({
  params,
  searchParams,
}: {
  params: Promise<{ league: string }>;
  searchParams: Promise<SearchParams>;
}) {
  const league = (await params).league as League;
  if (!["il", "euroleague", "eurocup"].includes(league)) notFound();
  const sp = await searchParams,
    season = typeof sp.season === "string" ? sp.season : defaultSeason(league);
  if (
    !/^\d{4}$/.test(season) ||
    (league === "il" && !ui.ilSeasons.includes(season))
  )
    notFound();
  const teams = homeTeams(season);
  const specified = typeof sp.team_id === "string" ? sp.team_id : "";
  const team =
    specified === "all"
      ? ""
      : teams.some((t) => String(t.id) === specified)
        ? specified
        : automaticTeams.get(season)!;
  const url = league === "il" && team ? homePath(season, team) : null;
  const data =
    url && (await headers()).get("x-ibpl-client-data") !== "1"
      ? await homeAnswer(new URLSearchParams({ season, team_id: team })).catch(
          () => null,
        )
      : null;
  return (
    <Suspense fallback={null}>
      <HomeClient
        league={league}
        initialSeason={season}
        initialTeam={team}
        initial={{ url, data }}
      />
    </Suspense>
  );
}
