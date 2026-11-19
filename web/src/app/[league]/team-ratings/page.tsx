import { Suspense } from "react";
import { headers } from "next/headers";
import { notFound } from "next/navigation";
import TeamRatingsClient from "@/components/TeamRatingsClient";
import type { League } from "@/rules/gameLogs";
import type { SearchParams } from "@/lib/tabQuery";
import { FirstReadFailed, TAB_FIRST_PATH } from "@/lib/tabPages";
import { teamRatingsAnswer } from "@/server/teamRatingsRepo";
import { pageMetadata } from "@/lib/pageTitle";
import { pageFailed } from "@/server/requestLog";

export const generateMetadata = pageMetadata("team-ratings");
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
  const path = TAB_FIRST_PATH["team-ratings"](l, await searchParams);
  const data =
    (await headers()).get("x-ibpl-client-data") === "1"
      ? null
      : await teamRatingsAnswer(
          l,
          new URL(path, "http://local").searchParams,
        ).catch((e) => {
          pageFailed("team-ratings", l)(e);
          throw new FirstReadFailed();
        });
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
