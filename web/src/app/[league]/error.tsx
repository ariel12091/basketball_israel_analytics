"use client";
import { Suspense } from "react";
import { useParams, usePathname, useSearchParams } from "next/navigation";
import GameLogsClient from "@/components/GameLogsClient";
import LineupsClient from "@/components/LineupsClient";
import OnOffClient from "@/components/OnOffClient";
import PlayerStatsClient from "@/components/PlayerStatsClient";
import TeamRatingsClient from "@/components/TeamRatingsClient";
import type { League } from "@/rules/gameLogs";
import { TAB_FIRST_PATH, type TablePage } from "@/lib/tabPages";

const CLIENTS = {
  "game-logs": GameLogsClient,
  onoff: OnOffClient,
  lineups: LineupsClient,
  "team-ratings": TeamRatingsClient,
  "player-stats": PlayerStatsClient,
};

// A table tab whose first server read failed (lib/tabPages FirstReadFailed)
// answers 500 so the CDN does not share it, and still shows the tab: the
// client fetches the table itself, as it did when the page rendered without
// it. Any other tab shows the error with a retry.
export default function TabError({ reset }: { reset: () => void }) {
  const league = useParams<{ league: string }>().league as League;
  const tab = usePathname().split("/")[2] as TablePage;
  const search = useSearchParams();
  const Client = CLIENTS[tab];
  if (!Client)
    return (
      <p role="alert">
        Something went wrong.{" "}
        <button type="button" onClick={reset}>
          Retry
        </button>
      </p>
    );
  const url = TAB_FIRST_PATH[tab](league, Object.fromEntries(search));
  return (
    <Suspense fallback={null}>
      <Client key={league} league={league} initial={{ url, data: null }} />
    </Suspense>
  );
}
