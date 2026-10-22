import { Suspense } from "react";
import { headers } from "next/headers";
import { notFound } from "next/navigation";
import type { League } from "@/rules/gameLogs";
import {
  PLAYER_QUERY_DEFAULTS,
  playerStatsPath,
  queryFromSearch,
  type SearchParams,
} from "@/lib/tabQuery";
import { playerStatsAnswer } from "@/server/playerStatsRepo";
import PlayerStatsClient from "@/components/PlayerStatsClient";
export default async function PlayerStatsPage({
  params,
  searchParams,
}: {
  params: Promise<{ league: string }>;
  searchParams: Promise<SearchParams>;
}) {
  const { league } = await params;
  if (!["il", "euroleague", "eurocup"].includes(league)) notFound();
  const l = league as League,
    path = playerStatsPath(
      l,
      queryFromSearch(l, await searchParams, PLAYER_QUERY_DEFAULTS),
    );
  const data =
    (await headers()).get("x-ibpl-client-data") === "1"
      ? null
      : await playerStatsAnswer(
          l,
          new URL(path, "http://local").searchParams,
        ).catch(() => null);
  return (
    <Suspense fallback={null}>
      <PlayerStatsClient
        key={league}
        league={l}
        initial={{ url: path, data }}
      />
    </Suspense>
  );
}
