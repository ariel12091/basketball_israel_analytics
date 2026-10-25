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
    })
      .then((r) => (r.ok ? (r.json() as Promise<VersionPayload>) : null))
      .then((value) => {
        if (alive && active === controller && !controller.signal.aborted)
          onValue(value);
      })
      .catch(() => {});
  };
  check();
  const timer = setInterval(check, 60_000);
  window.addEventListener("focus", check);
  return () => {
    alive = false;
    active?.abort();
    clearInterval(timer);
    window.removeEventListener("focus", check);
  };
}
