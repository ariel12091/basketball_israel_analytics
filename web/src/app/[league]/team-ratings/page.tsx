import { Suspense } from "react";
import { headers } from "next/headers";
import { notFound } from "next/navigation";
import type { League } from "@/rules/gameLogs";
import {
  TEAM_QUERY_DEFAULTS,
  teamRatingsPath,
  queryFromSearch,
  type SearchParams,
} from "@/lib/tabQuery";
import { teamRatingsAnswer } from "@/server/teamRatingsRepo";
import TeamRatingsClient from "@/components/TeamRatingsClient";
export default async function TeamRatingsPage({
  params,
  searchParams,
}: {
  params: Promise<{ league: string }>;
  searchParams: Promise<SearchParams>;
}) {
  const { league } = await params;
  if (!["il", "euroleague", "eurocup"].includes(league)) notFound();
  const l = league as League;
  const path = teamRatingsPath(
    l,
    queryFromSearch(l, await searchParams, TEAM_QUERY_DEFAULTS),
  );
  const data =
    (await headers()).get("x-ibpl-client-data") === "1"
      ? null
      : await teamRatingsAnswer(
          l,
          new URL(path, "http://local").searchParams,
        ).catch(() => null);
  return (
    <Suspense fallback={null}>
      <TeamRatingsClient
        key={league}
        league={l}
        initial={{ url: path, data }}
      />
    </Suspense>
  );
}
