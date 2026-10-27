import { Suspense } from "react";
import { headers } from "next/headers";
import { notFound } from "next/navigation";
import PlayerStatsClient from "@/components/PlayerStatsClient";
import type { League } from "@/rules/gameLogs";
import type { SearchParams } from "@/lib/tabQuery";
import { FirstReadFailed, TAB_FIRST_PATH } from "@/lib/tabPages";
import { playerStatsAnswer } from "@/server/playerStatsRepo";
export default async function PlayerStatsPage({
  params,
  searchParams,
}: {
  params: Promise<{ league: string }>;
  searchParams: Promise<SearchParams>;
}) {
  const { league } = await params;
  if (!["il", "euroleague", "eurocup"].includes(league)) notFound();
  const l = league as League;
  const path = TAB_FIRST_PATH["player-stats"](l, await searchParams);
  const data =
    (await headers()).get("x-ibpl-client-data") === "1"
      ? null
      : await playerStatsAnswer(
          l,
          new URL(path, "http://local").searchParams,
        ).catch(() => {
          throw new FirstReadFailed();
        });
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
