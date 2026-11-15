import { noteVersion } from "./dataVersion";
type VersionPayload = { leagues?: { league: string; version: string }[] };
/** Latest poll owns its result; cleanup also invalidates already-started JSON. */
export function pollVersions(onValue: (value: VersionPayload | null) => void) {
  let alive = true;
  let active: AbortController | undefined;
  const check = () => {
    active?.abort();
    const controller = new AbortController();
    active = controller;
    void fetch("/api/v1/version", {
      signal: controller.signal,
      cache: "no-store",
      // Background bookkeeping: never ahead of the page's own requests.
      priority: "low",
    })
      .then((r) => (r.ok ? (r.json() as Promise<VersionPayload>) : null))
      .then((value) => {
        for (const x of value?.leagues ?? []) noteVersion(x.league, x.version);
        if (alive && active === controller && !controller.signal.aborted)
          onValue(value);
      })
      .catch(() => {});
  };
  // A hidden tab skips its polls and catches up the moment it is shown.
  const hidden = () =>
    typeof document !== "undefined" && document.visibilityState === "hidden";
  const whenShown = () => {
    if (!hidden()) check();
  };
  check();
  const timer = setInterval(whenShown, 60_000);
  window.addEventListener("focus", check);
  if (typeof document !== "undefined")
    document.addEventListener("visibilitychange", whenShown);
  return () => {
    alive = false;
    active?.abort();
    clearInterval(timer);
    window.removeEventListener("focus", check);
    if (typeof document !== "undefined")
      document.removeEventListener("visibilitychange", whenShown);
  };
}
