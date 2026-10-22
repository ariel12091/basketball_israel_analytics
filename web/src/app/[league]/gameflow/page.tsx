import { headers } from "next/headers";
import { notFound } from "next/navigation";
import { gameflowAnswer, parseGameflow } from "@/server/gameflowRepo";
import type { League } from "@/rules/gameLogs";
import type { SearchParams } from "@/lib/tabQuery";
import GameflowClient from "@/components/GameflowClient";
export default async function GameflowPage({
  params,
  searchParams,
}: {
  params: Promise<{ league: string }>;
  searchParams: Promise<SearchParams>;
}) {
  const { league } = await params;
  if (!["il", "euroleague", "eurocup"].includes(league)) notFound();
  const l = league as League,
    sp = await searchParams,
    s = new URLSearchParams();
  for (const k of ["game_id", "team_id"])
    if (typeof sp[k] === "string") s.set(k, sp[k]);
  let p;
  try {
    p = parseGameflow(s);
  } catch {
    notFound();
  }
  const url = `/api/v1/${l}/gameflow?game_id=${p.game}&team_id=${p.team}`,
    data =
      (await headers()).get("x-ibpl-client-data") === "1"
        ? null
        : await gameflowAnswer(l, s).catch(() => null);
  return (
    <GameflowClient
      league={l}
      game={p.game}
      team={p.team}
      initial={{ url, data }}
    />
  );
}
