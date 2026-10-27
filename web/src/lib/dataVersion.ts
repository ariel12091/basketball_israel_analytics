// The latest data version this browser has seen per league, sent as `v` on
// every API read so Vercel's CDN can keep each answer under its version
// (api.ts answerJson): a new ETL publication changes the address, so nothing
// cached is ever served for newer data. EuroLeague and EuroCup share one load.
const known = new Map<string, string>();
const scope = (league: string) => (league === "il" ? "il" : "euro");

export function noteVersion(league: string, version: string | undefined) {
  if (version && version !== "unknown") known.set(scope(league), version);
}

/** `url` with `v=<version>` when this browser knows its league's version. */
export function withVersion(url: string): string {
  const league = /^\/api\/v1\/([a-z]+)\//.exec(url)?.[1];
  const version = league && known.get(scope(league));
  if (!version) return url;
  return `${url}${url.includes("?") ? "&" : "?"}v=${encodeURIComponent(version)}`;
}

/** The league an API path reads, for noting an answer's version. */
export const leagueOf = (url: string) =>
  /^\/api\/v1\/([a-z]+)\//.exec(url)?.[1] ?? "";
