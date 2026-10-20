import { Suspense } from "react";
import { notFound } from "next/navigation";
import GameLogsClient from "@/components/GameLogsClient";
import type { League } from "@/rules/gameLogs";
export default async function GameLogsPage({
  params,
}: {
  params: Promise<{ league: string }>;
}) {
  const { league } = await params;
  if (!["il", "euroleague", "eurocup"].includes(league)) notFound();
  return (
    <Suspense
      fallback={<main className="loading-page">Loading Game Logs…</main>}
    >
      <GameLogsClient key={league} league={league as League} />
    </Suspense>
  );
}
