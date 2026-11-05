import { test, expect, type Page } from "@playwright/test";
const presets = [
  ["starters_bench", "own_starters", "gte:3", "lte:2", "Starters", "Bench"],
  [
    "opp_starters_bench",
    "opp_starters",
    "gte:3",
    "lte:2",
    "vs Starters",
    "vs Bench",
  ],
  ["clutch", "clutch", "true", "false", "Clutch", "Non-Clutch"],
  ["home_away", "home_away", "home", "away", "Home", "Away"],
  ["win_loss", "outcome", "win", "loss", "Win", "Loss"],
  [
    "top_bottom_rank",
    "opp_rank_side",
    "top",
    "bottom",
    "vs Top 4",
    "vs Bottom 4",
  ],
  [
    "date_split",
    "end",
    "2026-01-01",
    "2026-07-01",
    "Before Jan 01",
    "From Jan 01",
  ],
  ["gn_split", "max_gn", "10", "", "GN 1-10", "GN 11+"],
];
async function stub(page: Page) {
  const seen: URLSearchParams[] = [];
  await page.route("**/api/v1/**", (route) => {
    const p = new URL(route.request().url()).searchParams;
    if (route.request().url().includes("/version"))
      return route.fulfill({ json: { leagues: [] } });
    if (p.get("kind") !== "refs") seen.push(p);
    return route.fulfill({
      json: {
        rows:
          p.get("kind") === "refs"
            ? []
            : [
                {
                  team_id: 2,
                  team_name: "Audit team",
                  net_rtg: 8,
                  off_ppp: 110,
                  def_ppp: 102,
                  off_poss: 100,
                  def_poss: 100,
                  sub_lineup_hash: "1,2,3,4,5",
                  player_names_str: "One, Two, Three, Four, Five",
                },
              ],
        teams: [{ team_id: 2, team_name: "Audit team" }],
        players: [],
        rounds: [1, 10, 20],
        meta: { version: "audit", key: p.toString() },
      },
    });
  });
  return seen;
}
/** Phones fold the Compare filters away. */
async function openFilters(page: Page) {
  const toggle = page.getByRole("button", { name: "Show filters" });
  await page.locator(".cmp-grid").waitFor();
  if (await toggle.isVisible()) await toggle.click();
}
async function pick(page: Page, id: string, label: string) {
  await openFilters(page);
  const root = page.locator(`[data-input-id="${id}"]`);
  await root.locator(".selectize-input").click();
  await root.getByRole("option", { name: label, exact: true }).click();
}
for (const [preset, key, a, b, labelA, labelB] of presets)
  test(`[CMP-02][CMP-03][CMP-06][X-18][league:il] preset ${preset} restores side requests and labels`, async ({
    page,
  }) => {
    const seen = await stub(page);
    await page.goto(
      `/il/compare?season=2026&preset=${preset}&split_date=2026-01-01&split_gn=10`,
    );
    await expect(page.locator('th[data-column="metric_a"]')).toContainText(
      labelA,
    );
    await expect(page.locator('th[data-column="metric_b"]')).toContainText(
      labelB,
    );
    await expect
      .poll(() => seen.some((p) => (p.get(key) ?? "") === a))
      .toBe(true);
    await expect
      .poll(() => seen.some((p) => (p.get(key) ?? "") === b))
      .toBe(true);
    if (preset === "date_split")
      expect(seen.some((p) => p.get("start") === "2026-01-02")).toBe(true);
    if (preset === "gn_split")
      expect(seen.some((p) => p.get("min_gn") === "11")).toBe(true);
    expect(
      await page.evaluate(
        () => document.documentElement.scrollWidth <= innerWidth + 1,
      ),
    ).toBe(true);
  });
test("[CMP-01][CMP-05][CMP-07][CMP-08][league:il] controls mirror, rearm auto and reset modes", async ({
  page,
}) => {
  await stub(page);
  await page.goto("/il/compare?season=2026&preset=home_away");
  await expect(page.locator('td[data-column="entity_name"]')).toBeVisible();
  // Min possessions is a stepper in the toolbar: typed values apply on Enter.
  await page.locator("#cmp_min_poss").fill("50");
  await page.locator("#cmp_min_poss").press("Enter");
  await expect(page).toHaveURL(/auto_min=false/);
  await pick(page, "cmp_a_outcome", "Win");
  await expect
    .poll(() => new URL(page.url()).searchParams.get("b_outcome"))
    .toBe("win");
  await expect(page).not.toHaveURL(/auto_min=false/);
  await page
    .getByRole("group", { name: "Comparison metric", exact: true })
    .getByRole("button", { name: "Offense", exact: true })
    .click();
  await expect(page).toHaveURL(/metric=off_ppp/);
  if (page.viewportSize()!.width < 1000)
    await page.getByRole("button", { name: "Toggle navigation" }).click();
  else await page.locator(".nav-link.active").hover();
  await page
    .getByRole("menuitemradio", { name: "Lineups", exact: true })
    .click();
  await expect(page).toHaveURL(/view=Lineups/);
  const q = new URL(page.url()).searchParams;
  expect(q.get("preset")).toBeNull();
  expect(q.get("a_outcome")).toBeNull();
  expect(q.get("b_outcome")).toBeNull();
  expect(q.get("metric")).toBeNull();
  await expect(page.locator("#cmp_min_poss")).toHaveValue("10");
});

test("[CMP-02][league:il] every filter says what it is, with A beside B", async ({
  page,
}) => {
  await stub(page);
  await page.goto("/il/compare?season=2026&preset=starters_bench");
  await openFilters(page);
  const rows = page.locator(".cmp-grid-row");
  await expect(rows.first()).toBeVisible();
  const count = await rows.count();
  expect(count).toBeGreaterThan(8);
  for (let i = 0; i < count; i++) {
    const row = rows.nth(i);
    await expect(row.locator(".cmp-grid-hint")).toHaveText(/\S/);
    const a = await row.locator(".cmp-grid-cell.is-a").boundingBox();
    const b = await row.locator(".cmp-grid-cell.is-b").boundingBox();
    expect(b!.x).toBeGreaterThanOrEqual(a!.x + a!.width);
    expect(Math.abs(b!.y - a!.y)).toBeLessThan(2);
  }
  const differing = page.locator(".cmp-grid-row.is-diff");
  await expect(differing).toHaveCount(1);
  await expect(differing.locator(".cmp-grid-name")).toHaveText(
    "Own lineup starters",
  );
  await expect(page.locator(".cmp-grid-head.is-b")).toContainText("Bench");
  // The mode text stays clear of the dropdown arrow, down to a 360px phone.
  const box = page
    .locator('[data-input-id="cmp_b_num_starters_off_mode"] .selectize-input')
    .first();
  const clearance = () =>
    box.evaluate((el) => {
      const after = getComputedStyle(el, "::after"),
        input = el.getBoundingClientRect(),
        arrowLeft =
          input.right - parseFloat(after.right) - parseFloat(after.width);
      return (
        arrowLeft - el.querySelector(".item")!.getBoundingClientRect().right
      );
    });
  expect(await clearance()).toBeGreaterThanOrEqual(0);
  await page.setViewportSize({ width: 360, height: 800 });
  await openFilters(page);
  expect(await clearance()).toBeGreaterThanOrEqual(0);
  expect(
    await page.evaluate(
      () => document.documentElement.scrollWidth - window.innerWidth,
    ),
  ).toBe(0);
});

test("[CMP-02][league:il] phones fold the filters and keep what is compared", async ({
  page,
}, info) => {
  test.skip(info.project.name !== "phone");
  await stub(page);
  await page.goto("/il/compare?season=2026&preset=starters_bench");
  await expect(page.locator(".cmp-grid-head.is-b")).toContainText("Bench");
  await expect(page.locator(".cmp-grid-row").first()).toBeHidden();
  await expect(page.locator(".cmp-grid-group")).toBeHidden();
  await page.getByRole("button", { name: "Show filters" }).click();
  await expect(page.locator(".cmp-grid-row").first()).toBeVisible();
  await expect(page.locator(".cmp-grid-group")).toBeVisible();
  await page.getByRole("button", { name: "Hide filters" }).click();
  await expect(page.locator(".cmp-grid-row").first()).toBeHidden();
});

test("[CMP-02][league:il] opponent rank reads as one filter and says when it is not applied", async ({
  page,
}) => {
  await stub(page);
  await page.goto("/il/compare?season=2026&a_opp_rank_side=top");
  await openFilters(page);
  const group = page.getByRole("group", { name: "Opponent Strength" });
  await expect(group.locator(".cmp-grid-name").first()).toHaveText(
    "Opponent Strength",
  );
  for (const name of ["Top / Bottom", "Rank N", "Metric"])
    await expect(group.getByRole("group", { name })).toHaveCount(1);
  // Only side A is half set, and only A says so.
  const warnings = group.locator(".cmp-grid-warning");
  await expect(warnings.first()).toHaveText("Not applied: pick Rank N");
  await expect(warnings.last()).toBeEmpty();
  await pick(page, "cmp_a_opp_rank_n", "4");
  await expect(warnings).toHaveCount(0);
  // The box does not push its controls out of line with the rows above.
  const outside = await page
    .locator(".cmp-grid-row", { hasText: "Win/Loss" })
    .locator(".cmp-grid-cell.is-b")
    .boundingBox();
  const inside = await group
    .getByRole("group", { name: "Metric" })
    .locator(".cmp-grid-cell.is-b")
    .boundingBox();
  expect(Math.abs(inside!.x - outside!.x)).toBeLessThan(1);
  expect(Math.abs(inside!.width - outside!.width)).toBeLessThan(1);
});

test("[CMP-02][league:il] desktop can hide the filters and give the table the width", async ({
  page,
}, info) => {
  test.skip(info.project.name !== "desktop");
  await stub(page);
  await page.goto("/il/compare?season=2026&preset=starters_bench");
  const firstRow = page.locator(".cmp-grid-row").first(),
    results = page.locator(".cmp-results"),
    grid = page.locator(".cmp-grid");
  await expect(firstRow).toBeVisible();
  const opened = (await results.boundingBox())!;
  expect(opened.x).toBeGreaterThan((await grid.boundingBox())!.x + 500);
  await page.getByRole("button", { name: "Hide filters" }).click();
  await expect(firstRow).toBeHidden();
  await expect(page.locator(".cmp-grid-group")).toBeHidden();
  await expect(page.locator(".cmp-grid-head.is-b")).toContainText("Bench");
  const closed = (await results.boundingBox())!;
  expect(closed.width).toBeGreaterThan(opened.width + 400);
  expect((await grid.boundingBox())!.height).toBeLessThan(110);
  await page.getByRole("button", { name: "Show filters" }).click();
  await expect(firstRow).toBeVisible();
});
