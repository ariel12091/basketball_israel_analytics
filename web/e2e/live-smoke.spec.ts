import type { Page } from "@playwright/test";
import { test, expect } from "./preview";
/** Opens the filters panel: every tab is on the drawer layout (Filters
 * button at the toolbar's right end), as lineup-controls.spec.ts does. */
async function openFilters(page: Page) {
  const button = page.locator(".chips-drawer-toggle");
  if ((await button.getAttribute("aria-expanded")) !== "true")
    await button.click();
  await expect(page.getByRole("dialog", { name: "Filters" })).toBeVisible();
}
for (const [league, season] of [
  ["il", 2026],
  ["euroleague", 2025],
  ["eurocup", 2025],
] as const) {
  test(`live ${league}: Player Stats server render`, async ({ page }, info) => {
    test.skip(
      process.env.LIVE_DB_SMOKE !== "1",
      "Read-only database smoke is opt-in",
    );
    await page.setExtraHTTPHeaders({});
    const errors: string[] = [];
    page.on("pageerror", (e) => errors.push(e.message));
    const response = await page.goto(
      `/${league}/player-stats?season=${season}`,
    );
    expect(await response!.text()).toContain('data-column="Player"');
    await expect(
      page.locator('.ibpl-dt-scroll tbody td[data-column="Player"]').first(),
    ).toBeVisible();
    expect(
      await page.evaluate(
        () => document.documentElement.scrollWidth <= innerWidth + 1,
      ),
    ).toBe(true);
    await page.screenshot({
      path: `test-results/live-player-${league}-${info.project.name}.png`,
      fullPage: true,
    });
    expect(errors).toEqual([]);
  });
}
test.skip(!process.env.LIVE_DB_SMOKE, "Opt-in read-only live database checks");
// The server renders the first table from the database here.
test.use({ extraHTTPHeaders: {} });
for (const [league, season] of [
  ["il", 2026],
  ["euroleague", 2025],
  ["eurocup", 2025],
] as const)
  test(`live ${league}: Team Ratings server render`, async ({ page }, info) => {
    const errors: string[] = [];
    page.on("pageerror", (e) => errors.push(e.message));
    const response = await page.goto(
      `/${league}/team-ratings?season=${season}`,
    );
    expect(await response!.text()).toContain('data-column="team_name"');
    await expect(
      page.locator('tbody td[data-column="team_name"]').first(),
    ).toBeVisible({ timeout: 30000 });
    if (info.project.name === "phone") {
      await openFilters(page);
      await page.keyboard.press("Escape");
    }
    expect(
      await page.evaluate(
        () => document.documentElement.scrollWidth <= innerWidth,
      ),
    ).toBe(true);
    await page.screenshot({
      path: `../output/playwright/live-team-${league}-${info.project.name}.png`,
      fullPage: true,
    });
    expect(errors).toEqual([]);
  });
for (const [league, season] of [
  ["il", 2026],
  ["euroleague", 2025],
  ["eurocup", 2025],
] as const)
  test(`live ${league}: Lineups server render and modal`, async ({
    page,
  }, info) => {
    const errors: string[] = [];
    page.on("pageerror", (e) => errors.push(e.message));
    const rosterReady = page.waitForResponse(
      (r) => r.url().includes("/lineup-roster") && r.ok(),
    );
    await page.goto(`/${league}/lineups?season=${season}`);
    await rosterReady;
    await expect(
      page.locator(".ibpl-dt-scroll tbody tr").first(),
    ).toContainText("TOTAL", { timeout: 30000 });
    const lineup = page.locator("button.ibpl-lineup-link").first();
    await lineup.scrollIntoViewIfNeeded();
    await expect(lineup).toBeInViewport();
    await lineup.evaluate(
      () =>
        new Promise<void>((resolve) =>
          requestAnimationFrame(() => requestAnimationFrame(() => resolve())),
        ),
    );
    await lineup.click();
    // The game log opens in place beneath its lineup (no modal).
    const log = page.locator(".ibpl-row-detail .ibpl-lineup-games");
    await expect(log.locator("tbody tr").first()).toBeVisible({
      timeout: 30000,
    });
    await page.screenshot({
      path: `../output/playwright/live-lineups-${league}-${info.project.name}.png`,
      fullPage: true,
    });
    await log.getByRole("button", { name: "Close game log" }).click();
    await expect(log).toHaveCount(0);
    expect(
      await page.evaluate(
        () => document.documentElement.scrollWidth <= innerWidth,
      ),
    ).toBe(true);
    expect(errors).toEqual([]);
  });
for (const [league, season] of [
  ["il", 2026],
  ["euroleague", 2025],
  ["eurocup", 2025],
]) {
  test(`live ${league}: data and phone/desktop interactions`, async ({
    page,
  }, info) => {
    page.on("pageerror", (e) => console.error("PAGE ERROR", e.message));
    page.on("console", (m) => {
      if (m.type() === "error") console.error("CONSOLE", m.text());
    });
    page.on("requestfailed", (r) =>
      console.error("REQUEST FAILED", r.url(), r.failure()?.errorText),
    );
    page.on("response", (r) => {
      if (r.url().includes("/api/"))
        console.log("API RESPONSE", r.status(), r.url());
    });
    await page.goto(`/${league}/game-logs?season=${season}`);
    await expect(page.locator(".ibpl-dt-scroll tbody tr").first()).toBeVisible({
      timeout: 30000,
    });
    await expect(page.locator(".dataTables_info")).toContainText("entries");
    if (info.project.name === "phone") await openFilters(page);
    await page.screenshot({
      path: `../output/playwright/live-${league}-${info.project.name}.png`,
      fullPage: true,
    });
  });
}

for (const [league, season] of [
  ["il", 2026],
  ["euroleague", 2025],
  ["eurocup", 2025],
]) {
  test(`live ${league}: On/Off server render`, async ({ page }, info) => {
    const errors: string[] = [];
    page.on("pageerror", (e) => errors.push(e.message));
    await page.goto(`/${league}/onoff?season=${season}`);
    await expect(
      page.locator('.ibpl-dt-scroll tbody td[data-column="Player"]').first(),
    ).toBeVisible({ timeout: 30000 });
    await expect(page.locator(".dataTables_info")).toContainText("entries");
    expect(
      await page.evaluate(
        () => document.documentElement.scrollWidth <= innerWidth,
      ),
    ).toBe(true);
    await page.screenshot({
      path: `../output/playwright/live-onoff-${league}-${info.project.name}.png`,
      fullPage: true,
    });
    expect(errors).toEqual([]);
  });
}

// Lineups "# Starters" and speed, both leagues, through the deployed API: full
// completed seasons, 2- and 3-player groups, the filters whose plans regressed
// on 2026-11-01 (EuroLeague migration 057: 34 s; Israeli fetch_lineups_all:
// 90 s+). Every request must answer within the 20 s budget, and no group may
// show more starters than players. Twin of app/tests/testthat/test-lineup-starters-db.R.
// Weekly only (LIVE_HEAVY=1, the workflow's Sunday run against production):
// the ten requests carry no data version, so the CDN never keeps them, and on
// every Preview push they cost about 2 GiB of database temp writes and reads
// (2026-11-14: 43 runs were ~95% of all filtered 2-3 player Lineups calls).
// SQL reaches the database without a web push, so a per-push run caught nothing
// a weekly one would miss.
for (const [league, season, filter] of [
  ["il", 2026, "&home_away=away"],
  ["il", 2026, "&clutch=true"],
  ["il", 2026, "&view=four_factors&last_n=5"],
  ["euroleague", 2025, "&home_away=away"],
  ["eurocup", 2025, "&last_n=10"],
] as const)
  for (const size of [2, 3])
    test(`live ${league}: ${size}-player lineups${filter} within budget`, async ({ page }, info) => {
      test.skip(process.env.LIVE_HEAVY !== "1", "Weekly heavy check (LIVE_HEAVY=1)");
      test.skip(info.project.name !== "desktop", "API check, once per run");
      const t0 = Date.now();
      const response = await page.goto(`/api/v1/${league}/lineups?season=${season}&size=${size}${filter}`);
      const ms = Date.now() - t0;
      expect(response!.status(), `${ms} ms`).toBe(200);
      expect(ms).toBeLessThan(20000);
      const { rows } = (await response!.json()) as { rows: { num_starters: number | null }[] };
      expect(rows.length).toBeGreaterThan(0);
      const over = rows.filter((r) => r.num_starters != null && r.num_starters > size + 1e-9);
      expect(over, "groups showing more starters than players").toEqual([]);
    });

// shape=table (docs/superpowers/plans/2026-11-15-web-api-complete.md): the
// API serves the page's finished table. Rule checks against the live data;
// the page and the API run the same options function (lib/pageState/).
for (const [league, season] of [
  ["il", 2026],
  ["euroleague", 2025],
] as const)
  test(`live ${league}: shape=table serves the finished tables`, async ({ page }, info) => {
    test.skip(process.env.LIVE_DB_SMOKE !== "1", "Read-only database smoke is opt-in");
    test.skip(info.project.name !== "desktop", "API check, once per run");
    const get = async (path: string) => {
      const r = await page.goto(`/api/v1/${league}/${path}`);
      expect(r!.status(), path).toBe(200);
      return ((await r!.json()) as { rows: Record<string, unknown>[]; meta: { shape?: string } });
    };
    // Player Stats: a minimum above every player's games is capped at the most played.
    const all = await get(`player-stats?season=${season}`);
    const most = Math.max(...all.rows.map((r) => Number(r.gp) || 0));
    const capped = await get(`player-stats?season=${season}&view=Totals&min_gp=999&shape=table`);
    expect(capped.meta.shape).toBe("table");
    expect(capped.rows.length).toBeGreaterThan(0);
    expect(capped.rows.every((r) => Number(r.gp) === most)).toBe(true);
    // Team Ratings: a stat filter narrows the league table.
    const stats = encodeURIComponent('[{"col":"net_rtg","op":"ge","value":0}]');
    const teams = await get(`team-ratings?season=${season}&shape=table&stats=${stats}`);
    expect(teams.rows.length).toBeGreaterThan(0);
    expect(teams.rows.every((r) => Number(r.net_rtg) >= 0)).toBe(true);
    // Hebrew names are Israeli only.
    if (league !== "il")
      expect(teams.rows.some((r) => "team_name_he" in r)).toBe(false);
    // A key the page does not read is refused, not ignored.
    const bad = await page.goto(`/api/v1/${league}/team-ratings?season=${season}&shape=table&sats=1`);
    expect(bad!.status()).toBe(400);
  });

// Hebrew team names (server/teamNames.ts): every Israeli answer carries them,
// EuroLeague none.
test("live: Israeli answers carry Hebrew team names, EuroLeague none", async ({ page }, info) => {
  test.skip(process.env.LIVE_DB_SMOKE !== "1", "Read-only database smoke is opt-in");
  test.skip(info.project.name !== "desktop", "API check, once per run");
  const json = async (path: string) => {
    const r = await page.goto(`/api/v1/${path}`);
    expect(r!.status(), path).toBe(200);
    return (await r!.json()) as { rows?: Record<string, unknown>[]; frames?: Record<string, Record<string, unknown>[]> };
  };
  const he = (rows: Record<string, unknown>[] | undefined, key = "team_name_he") =>
    (rows ?? []).some((r) => /[֐-׿]/.test(String(r[key] ?? "")));
  for (const path of ["il/team-ratings?season=2026", "il/player-stats?season=2026", "il/onoff?season=2026", "il/lineups?season=2026", "il/compare?season=2026&kind=ratings"])
    expect(he((await json(path)).rows), path).toBe(true);
  expect(he((await json("il/game-logs?season=2026&team=2")).rows, "opp_team_name_he"), "game logs opponent").toBe(true);
  expect(he((await json("il/home?season=2026&team_id=2")).frames?.ratings), "home ratings frame").toBe(true);
  const euro = await json("euroleague/team-ratings?season=2025");
  expect((euro.rows ?? []).some((r) => "team_name_he" in r)).toBe(false);
});

// format=csv: the finished table as a CSV download (server/csvAnswer.ts).
test("live: format=csv serves the table as CSV", async ({ page }, info) => {
  test.skip(process.env.LIVE_DB_SMOKE !== "1", "Read-only database smoke is opt-in");
  test.skip(info.project.name !== "desktop", "API check, once per run");
  // A download would end a navigation, and page.request skips the Preview
  // bypass (preview.ts routes the browser context): fetch from a page instead.
  await page.goto("/api/v1/il/team-ratings?season=2026&shape=table");
  const r = await page.evaluate(async () => {
    const res = await fetch("/api/v1/il/team-ratings?season=2026&format=csv");
    return {
      status: res.status,
      type: res.headers.get("content-type"),
      disposition: res.headers.get("content-disposition"),
      text: await res.text(),
    };
  });
  expect(r.status).toBe(200);
  expect(r.type).toBe("text/csv; charset=utf-8");
  expect(r.disposition).toBe('attachment; filename="team-ratings_il_2026.csv"');
  const lines = r.text.split("\r\n");
  expect(lines[0].startsWith('"Team","GP"')).toBe(true);
  expect(lines.length).toBeGreaterThan(10);
});

// The OpenAPI description (server/openapi.ts) is served and lists every route.
test("live: /api/v1/openapi.json describes the API", async ({ page }, info) => {
  test.skip(process.env.LIVE_DB_SMOKE !== "1", "Read-only database smoke is opt-in");
  test.skip(info.project.name !== "desktop", "API check, once per run");
  const r = await page.goto("/api/v1/openapi.json");
  expect(r!.status()).toBe(200);
  const spec = (await r!.json()) as { openapi: string; paths: Record<string, unknown> };
  expect(spec.openapi).toBe("3.1.0");
  for (const route of ["team-ratings", "player-stats", "onoff", "lineups", "compare", "game-logs", "gameflow", "home", "guide"])
    expect(spec.paths, route).toHaveProperty([`/api/v1/{league}/${route}`]);
});

// No twin without a pin (docs/shared_definitions.md rule 4): a EuroLeague
// Compare side reads the EuroLeague tab's own reader, so with the same
// filters it returns the tab's numbers, row for row.
test("live: EuroLeague Compare sides equal their tabs", async ({ page }, info) => {
  test.skip(process.env.LIVE_DB_SMOKE !== "1", "Read-only database smoke is opt-in");
  test.skip(info.project.name !== "desktop", "API check, once per run");
  type Rows = Record<string, unknown>[];
  // The request client, not page.goto: a whole season's lineups is too large
  // for the browser inspector's cache, which evicts the body. It skips the
  // page routes, so it sends the Preview bypass itself (e2e/preview.ts).
  const secret = process.env.VERCEL_AUTOMATION_BYPASS_SECRET;
  const rows = async (path: string): Promise<Rows> => {
    const r = await page.request.get(`/api/v1/${path}`, {
      headers: secret ? { "x-vercel-protection-bypass": secret } : {},
    });
    expect(r.status(), path).toBe(200);
    return ((await r.json()) as { rows: Rows }).rows;
  };
  const same = (a: Rows, b: Rows, key: (r: Record<string, unknown>) => string, cols: string[], what: string) => {
    const tab = new Map(b.map((r) => [key(r), r]));
    expect(a.length, what).toBe(tab.size);
    for (const r of a)
      for (const c of cols) expect(r[c], `${what} ${key(r)} ${c}`).toEqual(tab.get(key(r))?.[c]);
  };
  for (const league of ["euroleague", "eurocup"])
    for (const extra of ["", "&home_away=home", "&clutch=true"]) {
      const s = `season=2025${extra}`;
      same(
        await rows(`${league}/compare?kind=ratings&${s}`),
        await rows(`${league}/team-ratings?${s}`),
        (r) => String(r.team_id),
        ["off_ppp", "def_ppp", "net_rtg"],
        `${league}${extra} ratings`,
      );
      same(
        await rows(`${league}/compare?kind=lineups&size=5&${s}`),
        (await rows(`${league}/lineups?size=5&auto=false&min_poss=0&${s}`)).filter((r) => r.sub_lineup_hash !== "TOTAL"),
        (r) => String(r.sub_lineup_hash),
        ["total_poss", "off_pts", "def_pts"],
        `${league}${extra} lineups`,
      );
      const players = (await rows(`${league}/player-stats?${s}`)).filter((r) => !r.is_multi_team_total);
      same(
        await rows(`${league}/compare?kind=players&${s}`),
        players,
        (r) => `${r.team_id}:${r.player_id}`,
        ["gp", "pts", "reb", "ast"],
        `${league}${extra} players`,
      );
    }
});
