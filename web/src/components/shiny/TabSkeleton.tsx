"use client";
// What a tab shows between a tap and its first answer: the bar and the tab's
// frame at once, a placeholder where the content will go. Rendered by the
// table pages while their server read runs (lib/streamedTab) and by the Home
// and Compare loading files, so the bottom tab bar never leaves the screen.
import { useParams, usePathname, useRouter, useSearchParams } from "next/navigation";
import type { League } from "@/rules/gameLogs";
import type { PagePath } from "@/lib/pageTitle";
import { defaultSeason } from "@/lib/tabQuery";
import ui from "@/data/shinyUi.json";
import { tabForPath } from "./Navbar";
import { TabFrame } from "./TabKit";

export default function TabSkeleton({ message = "Loading…" }: { message?: string }) {
  const router = useRouter();
  const league = useParams<{ league: string }>().league as League;
  const path = (usePathname().split("/")[2] ?? "home") as PagePath;
  const season = useSearchParams().get("season") ?? defaultSeason(league);
  const tab = tabForPath(path);
  return (
    <TabFrame
      league={league}
      tab={tab}
      page={path}
      paneValue={tab}
      season={season}
      seasons={league === "il" ? ui.ilSeasons : ui.euroStaticSeasons}
      onSeason={(s) => router.push(`/${league}/${path}?season=${s}`)}
      views={[]}
      view=""
      onView={() => {}}
      updated={null}
      filtersId="tab-loading-filters"
      sidebar={null}
      mainOnly
      main={
        <div className="ibpl-tab-loading" aria-busy="true">
          <p role="status">{message}</p>
          <div className="ibpl-tab-loading-rows" aria-hidden="true">
            {Array.from({ length: 8 }, (_, i) => (
              <span key={i}></span>
            ))}
          </div>
        </div>
      }
    />
  );
}
