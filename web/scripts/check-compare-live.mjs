// Sequential, single-reader acceptance checks. This is not a load test.
import assert from "node:assert/strict";
import { mkdir, writeFile } from "node:fs/promises";
const base = process.env.COMPARE_BASE_URL ?? "http://localhost:3100";
const checks = [];
for (const [name, query, expected] of [
  ["teams", "season=2026", "table"],
  ["starters preset", "season=2026&preset=starters_bench", "table"],
  ["lineups", "season=2026&view=Lineups&size=2&lu_team=2", "table"],
  ["default players", "season=2026&view=Players", "players"],
  [
    "self comparison",
    "season=2026&view=Players&player_compare=self&player_a=1043&auto_players=false",
    "players",
  ],
  [
    "Home payload",
    "season=2026&preset=starters_bench&team_id=2&open_detail=true",
    "detail",
  ],
]) {
  const started = performance.now(),
    response = await fetch(`${base}/il/compare?${query}`),
    html = await response.text();
  assert.equal(response.status, 200, name);
  assert.ok(
    html.includes(
      expected === "table"
        ? 'data-column="entity_name"'
        : expected === "players"
          ? 'class="ibpl-pvp-row"'
          : 'aria-label="Entity comparison detail"',
    ),
    `${name}: initial server-rendered markup`,
  );
  checks.push({
    name,
    type: "SSR",
    ms: Math.round(performance.now() - started),
  });
}
for (const kind of [
  "ratings",
  "four_factors",
  "lineups",
  "players",
  "player_ff",
  "onoff",
  "refs",
  "player_defaults",
]) {
  const query = new URLSearchParams({ season: "2026", teams: "2", kind });
  const started = performance.now(),
    response = await fetch(`${base}/api/v1/il/compare?${query}`),
    data = await response.json();
  assert.equal(response.status, 200, `${kind}: ${data.error ?? ""}`);
  assert.equal(data.meta.key, query.toString());
  assert.ok(Array.isArray(data.rows));
  assert.ok(
    kind === "refs" ? data.players.length > 0 : data.rows.length > 0,
    `${kind}: nonempty historical data`,
  );
  checks.push({
    name: kind,
    type: "API",
    rows: data.rows.length,
    ms: Math.round(performance.now() - started),
  });
}
for (const [path, status] of [
  ["/euroleague/compare", 404],
  ["/eurocup/compare", 404],
  ["/api/v1/euroleague/compare?season=2026", 400],
  ["/api/v1/il/compare?kind=invalid", 400],
  ["/api/v1/il/compare?season=2026&start=bad", 400],
]) {
  const response = await fetch(base + path);
  const html = await response.text();
  if (status === 404) {
    // Next's loading boundary may have streamed HTTP 200 before notFound().
    // Assert the actual rejection UI and noindex, not an impossible late status.
    assert.ok(response.status === 200 || response.status === 404, path);
    assert.ok(html.includes('content="noindex"') && html.includes("This page could not be found."), path);
    assert.ok(!html.includes('data-column="entity_name"'), path);
  } else assert.equal(response.status, status, path);
  checks.push({ name: path, type: "validation", status: response.status });
}
const target = new URL("../../output/compare/live.json", import.meta.url);
await mkdir(new URL(".", target), { recursive: true });
await writeFile(
  target,
  JSON.stringify(
    {
      base,
      checks,
      note: "Sequential acceptance reads; not scalability evidence.",
    },
    null,
    2,
  ),
);
console.log(
  `${checks.length} local production Compare checks passed (${checks.filter((c) => c.type === "SSR").length} SSR, ${checks.filter((c) => c.type === "API").length} API, ${checks.filter((c) => c.type === "validation").length} validation).`,
);
