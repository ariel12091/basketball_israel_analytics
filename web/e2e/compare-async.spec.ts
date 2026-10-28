import { test, expect, type Page, type Route } from "@playwright/test";

const row = (name: string, value: number) => ({
  team_id: 2,
  team_name: name,
  net_rtg: value,
  off_poss: 100,
});
const answer = (route: Route, rows: ReturnType<typeof row>[], version = "v1") =>
  route.fulfill({
    json: {
      rows,
      meta: {
        version,
        key: new URL(route.request().url()).searchParams.toString(),
      },
    },
  });
async function intercept(
  page: Page,
  handle: (route: Route) => Promise<unknown>,
) {
  await page.route("**/api/v1/**", async (route) => {
    if (!route.request().url().includes("/compare?"))
      return route.fulfill({ json: { leagues: [] } });
    return handle(route);
  });
}
const path = "/il/compare?season=2026&preset=home_away";

test("Compare starts both sides before references finish and waits for their pair", async ({
  page,
}) => {
  let releaseRefs!: () => void, releaseB!: () => void;
  const refs = new Promise<void>((resolve) => {
    releaseRefs = resolve;
  });
  const b = new Promise<void>((resolve) => {
    releaseB = resolve;
  });
  const seen: string[] = [];
  await intercept(page, async (route) => {
    const params = new URL(route.request().url()).searchParams;
    if (params.get("kind") === "refs") {
      await refs;
      return answer(route, []);
    }
    const side = params.get("home_away")!;
    seen.push(side);
    if (side === "away") await b;
    return answer(route, [row("Async team", side === "home" ? 8 : 3)]);
  });
  await page.goto(path);
  await expect.poll(() => [...new Set(seen)].sort()).toEqual(["away", "home"]);
  await expect(
    page.getByRole("status").filter({ hasText: "Loading Side B" }),
  ).toBeVisible();
  await expect(
    page.getByRole("button", { name: "Async team", exact: true }),
  ).toHaveCount(0);
  releaseB();
  await expect(
    page.getByRole("button", { name: "Async team", exact: true }),
  ).toBeVisible();
  releaseRefs();
  await expect(
    page.getByRole("status").filter({ hasText: "Reference data" }),
  ).toHaveCount(0);
  expect(
    await page.evaluate(
      () => document.documentElement.scrollWidth <= innerWidth,
    ),
  ).toBe(true);
});

test("Compare retries only the failed side", async ({ page }) => {
  const seen: string[] = [];
  let allowRetry = false;
  await intercept(page, async (route) => {
    const params = new URL(route.request().url()).searchParams;
    if (params.get("kind") === "refs") return answer(route, []);
    const side = params.get("home_away")!;
    seen.push(side);
    if (side === "away" && !allowRetry) {
      return route.fulfill({
        status: 503,
        json: { error: "Side unavailable" },
      });
    }
    return answer(route, [row("Retry team", 5)]);
  });
  await page.goto(path);
  await expect(
    page.getByRole("button", { name: "Retry Side B", exact: true }),
  ).toBeVisible();
  const homeAttempts = seen.filter((x) => x === "home").length;
  const awayAttempts = seen.filter((x) => x === "away").length;
  allowRetry = true;
  await page.getByRole("button", { name: "Retry Side B", exact: true }).click();
  await expect(
    page.getByRole("button", { name: "Retry team", exact: true }),
  ).toBeVisible();
  expect(seen.filter((x) => x === "home")).toHaveLength(homeAttempts);
  expect(seen.filter((x) => x === "away")).toHaveLength(awayAttempts + 1);
});

test("Compare refuses a pair from different versions", async ({ page }) => {
  await intercept(page, async (route) => {
    const params = new URL(route.request().url()).searchParams;
    if (params.get("kind") === "refs") return answer(route, []);
    return answer(
      route,
      [row("Mismatched team", 5)],
      params.get("home_away") === "home" ? "v1" : "v2",
    );
  });
  await page.goto(path);
  await expect(
    page.getByRole("button", { name: "Refresh both sides" }),
  ).toBeVisible();
  await expect(
    page.getByRole("button", { name: "Mismatched team", exact: true }),
  ).toHaveCount(0);
});

test("Compare ignores an old side after the date filters change", async ({
  page,
}, info) => {
  let releaseOld!: () => void;
  const old = new Promise<void>((resolve) => {
    releaseOld = resolve;
  });
  await intercept(page, async (route) => {
    const p = new URL(route.request().url()).searchParams;
    if (p.get("kind") === "refs") return answer(route, []);
    if (p.get("start") === "2025-09-01" && p.get("home_away") === "away")
      await old;
    return answer(route, [
      row(p.get("start") === "2025-11-01" ? "Current team" : "Old team", 5),
    ]);
  });
  await page.goto(path);
  if (info.project.name === "phone")
    await page
      .getByRole("button", { name: "Show Filters", exact: true })
      .click();
  const start = page.getByRole("textbox", {
    name: "Date range start",
    exact: true,
  });
  await start.fill("2025-11-01");
  await start.press("Enter");
  await expect(
    page.getByRole("button", { name: "Current team", exact: true }),
  ).toBeVisible();
  releaseOld();
  await expect(
    page.getByRole("status").filter({ hasText: "Loading Side" }),
  ).toHaveCount(0);
  await expect(
    page.getByRole("button", { name: "Old team", exact: true }),
  ).toHaveCount(0);
});

test("Compare details continue after ratings fail", async ({ page }, info) => {
  const seen = new Set<string>();
  await intercept(page, async (route) => {
    const p = new URL(route.request().url()).searchParams,
      kind = p.get("kind")!;
    seen.add(kind);
    if (kind === "refs") return answer(route, []);
    if (kind === "ratings" && p.get("home_away") === "home")
      return route.fulfill({
        status: 503,
        json: { error: "Ratings unavailable" },
      });
    return route.fulfill({
      json: {
        rows: [
          {
            ...row("Detail team", 5),
            off_efg: 55,
            off_fg2_att: 10,
            off_fg2_made: 5,
          },
        ],
        meta: { version: "v1", key: p.toString() },
      },
    });
  });
  await page.goto(path + "&cmp_view=detail&entity=2");
  await expect(
    page.getByRole("button", { name: "Retry A ratings", exact: true }),
  ).toBeVisible();
  await expect
    .poll(() => seen.has("four_factors") && seen.has("shooting"))
    .toBe(true);
  await expect(
    page
      .locator(".cmp-stat-row.cmp-col-a")
      .filter({ has: page.locator(".cmp-stat-label", { hasText: /^eFG%$/ }) }),
  ).toContainText("55.0%");
  const overflow = await page.evaluate(() =>
    [...document.querySelectorAll("body *")]
      .filter((el) => el.getBoundingClientRect().right > innerWidth + 1)
      .map(
        (el) =>
          `${el.tagName}.${el.className}: ${Math.round(el.getBoundingClientRect().width)} right ${Math.round(el.getBoundingClientRect().right)}`,
      )
      .slice(0, 12),
  );
  expect(overflow).toEqual([]);
  await page.screenshot({
    path: `../output/playwright/compare-detail-${info.project.name}.png`,
    fullPage: true,
  });
  expect(
    await page.evaluate(
      () => document.documentElement.scrollWidth <= innerWidth,
    ),
  ).toBe(true);
});

test("Players Compare uses the player layout and restricts additional reads to roster teams", async ({
  page,
}) => {
  const requests: URLSearchParams[] = [];
  await intercept(page, async (route) => {
    const p = new URL(route.request().url()).searchParams;
    requests.push(p);
    const meta = { version: "v1", key: p.toString() };
    if (p.get("kind") === "refs")
      return route.fulfill({
        json: {
          rows: [],
          teams: [{ team_id: 2, team_name: "Team" }],
          rounds: [],
          players: [
            { player_id: 1, team_id: 2, name: "Player A" },
            { player_id: 2, team_id: 2, name: "Player B" },
          ],
          meta,
        },
      });
    return route.fulfill({
      json: {
        rows: [
          {
            player_id: 1,
            team_id: 2,
            gp: 2,
            minutes: 30,
            pts: 30,
            stl: 4,
            dfl: 2,
            team_name: "Team",
            "Off eFG% Diff": 1.5,
          },
          {
            player_id: 2,
            team_id: 2,
            gp: 2,
            minutes: 30,
            pts: 20,
            stl: 2,
            dfl: 1,
            team_name: "Team",
            "Off eFG% Diff": 0.5,
          },
        ],
        meta,
      },
    });
  });
  await page.goto(
    "/il/compare?season=2026&view=Players&player_a=1&player_b=2&auto_players=false",
  );
  await expect(
    page.locator(".ibpl-pvp-row").filter({
      has: page.locator(".ibpl-pvp-label", { hasText: /^Points$/ }),
    }),
  ).toContainText("15.0");
  await expect(page.locator(".ibpl-pvp-badge").first()).toHaveText("+5.0");
  await page.getByRole("button", { name: "Four Factors", exact: true }).click();
  await expect
    .poll(() => requests.some((p) => p.get("kind") === "player_ff"))
    .toBe(true);
  expect(
    requests
      .filter((p) => p.get("kind") === "player_ff")
      .every((p) => p.get("teams") === "2"),
  ).toBe(true);
  expect(requests.some((p) => p.get("kind") === "player_defaults")).toBe(false);
  expect(
    await page.evaluate(
      () => document.documentElement.scrollWidth <= innerWidth,
    ),
  ).toBe(true);
});

test("Compare gap stays descending and the whole row opens detail", async ({
  page,
}) => {
  await intercept(page, async (route) => {
    const p = new URL(route.request().url()).searchParams;
    if (p.get("kind") === "refs") return answer(route, []);
    return answer(route, [
      row("First team", p.get("home_away") === "home" ? 8 : 3),
      { ...row("Second team", 3), team_id: 3 },
    ]);
  });
  await page.goto(path);
  const firstName = page.locator('tbody td[data-column="entity_name"]').first();
  await expect(firstName).toHaveText("First team");
  const gap = page.locator('th[data-column="gap"]');
  await gap.click();
  await gap.click();
  await expect(gap).toHaveAttribute("aria-sort", "descending");
  await expect(firstName).toHaveText("First team");
  await page.locator('tbody td[data-column="poss_a"]').first().click();
  await expect(
    page.getByRole("region", { name: "Entity comparison detail" }),
  ).toBeVisible();
  await page
    .getByRole("button", { name: "<- Back to league view", exact: true })
    .click();
  await firstName.focus();
  await firstName.press("Enter");
  await expect(
    page.getByRole("region", { name: "Entity comparison detail" }),
  ).toBeVisible();
});

test("Players Compare refreshes a data-version mismatch without mixing values", async ({
  page,
}) => {
  let refreshed = false;
  await intercept(page, async (route) => {
    const p = new URL(route.request().url()).searchParams,
      kind = p.get("kind");
    const meta = {
      key: p.toString(),
      version: kind === "players" && !refreshed ? "v1" : "v2",
    };
    if (kind === "refs")
      return route.fulfill({
        json: {
          rows: [],
          teams: [{ team_id: 2, team_name: "Team" }],
          rounds: [],
          players: [
            { player_id: 1, team_id: 2, name: "Player A" },
            { player_id: 2, team_id: 2, name: "Player B" },
          ],
          meta,
        },
      });
    return route.fulfill({
      json: {
        rows: [
          {
            player_id: 1,
            team_id: 2,
            gp: 2,
            minutes: 30,
            pts: 30,
            "Off eFG% Diff": 1.5,
          },
          {
            player_id: 2,
            team_id: 2,
            gp: 2,
            minutes: 30,
            pts: 20,
            "Off eFG% Diff": 0.5,
          },
        ],
        meta,
      },
    });
  });
  await page.goto(
    "/il/compare?season=2026&view=Players&player_a=1&player_b=2&auto_players=false&player_view=ff_swing",
  );
  const retry = page.getByRole("button", {
    name: "Refresh player comparison",
    exact: true,
  });
  await expect(retry).toBeVisible();
  const metric = page
    .locator(".ibpl-pvp-row")
    .filter({ has: page.locator(".ibpl-pvp-label", { hasText: /^eFG%$/ }) })
    .first();
  await expect(metric.locator(".ibpl-pvp-a")).toContainText("--");
  refreshed = true;
  await retry.click();
  await expect(retry).toHaveCount(0);
  await expect(metric.locator(".ibpl-pvp-a")).toContainText("+1.5");
});

test("Home storyline links pin both sides and open team detail", async ({
  page,
}) => {
  const requests: URLSearchParams[] = [];
  await intercept(page, async (route) => {
    const p = new URL(route.request().url()).searchParams;
    if (p.get("kind") === "refs") return answer(route, []);
    requests.push(p);
    return answer(route, [
      row("Pinned team", 5),
      { ...row("Other team", 3), team_id: 3 },
    ]);
  });
  await page.goto(
    "/il/compare?season=2026&view=Lineups&preset=starters_bench&team_id=2&open_detail=true",
  );
  await expect(
    page.getByRole("region", { name: "Entity comparison detail" }),
  ).toBeVisible();
  await expect(page.locator(".cmp-team-header")).toHaveText("Pinned team");
  expect(
    requests
      .filter((p) => p.get("kind") === "ratings")
      .every((p) => p.get("teams") === "2"),
  ).toBe(true);
  await expect(
    page.getByRole("columnheader", { name: "Lineup", exact: true }),
  ).toHaveCount(0);
});

test("Players self comparison uses separate date windows", async ({
  page,
}, info) => {
  const requests: URLSearchParams[] = [];
  await intercept(page, async (route) => {
    const p = new URL(route.request().url()).searchParams,
      meta = { version: "v1", key: p.toString() };
    if (p.get("kind") === "refs")
      return route.fulfill({
        json: {
          rows: [],
          teams: [{ team_id: 2, team_name: "Team" }],
          rounds: [],
          players: [{ player_id: 1, team_id: 2, name: "Self player" }],
          meta,
        },
      });
    requests.push(p);
    return route.fulfill({
      json: {
        rows: [
          {
            player_id: 1,
            team_id: 2,
            gp: 2,
            minutes: 30,
            pts: p.get("start") === "2025-11-01" ? 30 : 10,
          },
        ],
        meta,
      },
    });
  });
  await page.goto(
    "/il/compare?season=2026&view=Players&player_a=1&player_compare=self&auto_players=false&a_start=2025-11-01",
  );
  await expect(
    page.getByRole("heading", { name: "Self player", exact: true }),
  ).toHaveCount(2);
  await expect(page.locator(".ibpl-pvp-badge").first()).toHaveText("+10.0");
  expect(requests.some((p) => p.get("start") === "2025-11-01")).toBe(true);
  expect(requests.some((p) => p.get("start") === "2025-09-01")).toBe(true);
  if (info.project.name === "phone")
    await page
      .getByRole("button", { name: "Show Filters", exact: true })
      .click();
  await expect(
    page
      .getByRole("group", { name: "A", exact: true })
      .getByRole("textbox", { name: "Date range start", exact: true }),
  ).toHaveValue("2025-11-01");
  await expect(
    page
      .getByRole("group", { name: "B", exact: true })
      .getByRole("textbox", { name: "Date range start", exact: true }),
  ).toHaveValue("2025-09-01");
});

test("Team player drilldown changes rates, marks missing sides and clears on context edits", async ({
  page,
}, info) => {
  await intercept(page, async (route) => {
    const p = new URL(route.request().url()).searchParams;
    if (p.get("kind") === "refs") return answer(route, []);
    const home = p.get("home_away") === "home",
      meta = { version: "v1", key: p.toString() };
    const one = {
      team_id: 2,
      team_name: "Team",
      player_id: 1,
      player_name: "Player One",
      gp: home ? 2 : 1,
      minutes: home ? 60 : 20,
      poss_on_floor: home ? 100 : 50,
      pts: home ? 30 : 10,
      reb: 3,
      ast: 1,
      stl: 1,
      blk: 0,
      tov: 2,
      usg_pct: 15,
      fg_pct: 50,
      tp_pct: 40,
      ft_pct: 80,
      efg: 60,
      ts: 58,
    };
    return route.fulfill({
      json: {
        rows: home
          ? [
              one,
              {
                ...one,
                player_id: 2,
                player_name: "Player Two",
                gp: 1,
                pts: 5,
                poss_on_floor: 10,
              },
            ]
          : [one],
        meta,
      },
    });
  });
  await page.goto(
    path + "&cmp_view=players&entity=2&min_poss=0&auto_min=false",
  );
  await expect(page.locator("tbody")).toContainText("didn't play");
  await page
    .getByRole("button", { name: "Per 60 Possessions", exact: true })
    .click();
  const one = page
    .locator("tbody tr")
    .filter({
      has: page.getByRole("button", { name: "Player One", exact: true }),
    });
  await expect(one.locator('td[data-column="metric_a"]')).toHaveText("18.0");
  await page.getByRole("button", { name: "Player One", exact: true }).click();
  const detail = page.getByRole("region", {
    name: "Player comparison detail",
    exact: true,
  });
  await expect(detail).toBeVisible();
  await expect(detail).toContainText("Usage");
  await expect(detail).toContainText("Production");
  if (info.project.name === "phone")
    await page
      .getByRole("button", { name: "Show Filters", exact: true })
      .click();
  const start = page.getByRole("textbox", {
    name: "Date range start",
    exact: true,
  });
  await start.fill("2025-11-01");
  await start.press("Enter");
  await expect(detail).toHaveCount(0);
  await expect(
    page.getByRole("button", { name: "Player One", exact: true }),
  ).toBeVisible();
  await expect(page).not.toHaveURL(/drill_player=/);
});
