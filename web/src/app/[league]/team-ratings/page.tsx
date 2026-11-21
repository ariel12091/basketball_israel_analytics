import { Suspense } from "react";
import { notFound } from "next/navigation";
import TeamRatingsClient from "@/components/TeamRatingsClient";
import type { League } from "@/rules/gameLogs";
import type { SearchParams } from "@/lib/tabQuery";
import { TAB_FIRST_PATH } from "@/lib/tabPages";
import { teamRatingsAnswer } from "@/server/teamRatingsRepo";
import { pageMetadata } from "@/lib/pageTitle";
import { streamedTab } from "@/server/streamedTab";

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
  return streamedTab({
    page: "team-ratings",
    league: l,
    read: () => teamRatingsAnswer(l, new URL(path, "http://local").searchParams),
    render: (data) => (
      <Suspense fallback={null}>
        <TeamRatingsClient key={league} league={l} initial={{ url: path, data }} />
      </Suspense>
    ),
  });
}
