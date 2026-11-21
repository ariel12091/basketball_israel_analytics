// A table tab's page: its first table read on the server, rendered into the
// tab's client. Full page loads wait for the read, so a failed one answers
// 500 (lib/tabPages FirstReadFailed) and the CDN does not keep it. A tap on a
// tab bar link is a client navigation (Next sends the `rsc` header, the same
// one next.config.ts keys the CDN rule on); it gets the tab's frame at once
// and the table when the read finishes, so the tap never looks ignored.
import { Suspense, type ReactNode } from "react";
import { headers } from "next/headers";
import type { League } from "@/rules/gameLogs";
import { FirstReadFailed, type TablePage } from "@/lib/tabPages";
import { pageFailed } from "@/server/requestLog";
import TabSkeleton from "@/components/shiny/TabSkeleton";

export async function streamedTab<T>({
  page,
  league,
  read,
  render,
}: {
  page: TablePage;
  league: League;
  read: () => Promise<T>;
  render: (data: T | null) => ReactNode;
}) {
  const h = await headers();
  if (h.get("x-ibpl-client-data") === "1") return render(null);
  const answer = read().catch((e) => {
    pageFailed(page, league)(e);
    throw new FirstReadFailed();
  });
  if (h.get("rsc") !== "1") return render(await answer);
  return (
    <Suspense fallback={<TabSkeleton />}>
      <Deferred answer={answer} render={render} />
    </Suspense>
  );
}

async function Deferred<T>({
  answer,
  render,
}: {
  answer: Promise<T>;
  render: (data: T | null) => ReactNode;
}) {
  return render(await answer);
}
