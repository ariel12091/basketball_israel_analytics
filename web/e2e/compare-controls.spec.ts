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
async function pick(page: Page, id: string, label: string) {
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
  if (page.viewportSize()!.width < 1000)
    await page
      .getByRole("button", { name: "Show Filters", exact: true })
      .click();
  await page.locator("#cmp_min_poss").fill("50");
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
