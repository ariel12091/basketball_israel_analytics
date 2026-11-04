import { test, expect } from "@playwright/test";
import { loadGameflow } from "../test/gameflowGoldens";
const CHART = "svg.ibpl-ribbon:not(.ibpl-gameflow-gutter)";
// Hover moves nothing, so a bar stays where the pointer is on the way to a click.
// A click on a stint shrinks the chart for that stint's lineups, and only those.
test("Gameflow hover moves nothing; a clicked stint shows only that stint", async ({
  page,
}, info) => {
  test.skip(info.project.name === "phone", "phones keep the quarter cards");
  const { g, answer, url } = loadGameflow("euroleague_overtime.json"),
    height = 768;
  await page.setViewportSize({ width: 1366, height });
  await page.route("**/api/v1/**", (route) =>
    route.fulfill({
      json: new URL(route.request().url()).pathname.endsWith("/version")
        ? { leagues: [{ league: g.case.league, version: "golden" }] }
        : answer,
    }),
  );
  await page.goto(url);
  const chart = page.locator(CHART),
    details = page.locator(".gf-details"),
    tip = page.locator(".gf-hover-tip");
  await expect(chart).toHaveCount(1);
  const full = (await chart.boundingBox())!.height,
    lane = chart.locator(".ibpl-ribbon-lane.is-own").nth(5),
    aimed = (await lane.getAttribute("data-player-key"))!,
    from = Number(await lane.getAttribute("data-start")),
    to = Number(await lane.getAttribute("data-end")),
    box = (await lane.locator("rect").first().boundingBox())!,
    x = box.x + box.width / 2,
    y = box.y + box.height / 2;
  // Hover: a tooltip with the lineups, inside the window, and nothing moves.
  await page.mouse.move(x - 3, y);
  await page.mouse.move(x, y);
  await expect(tip).toBeVisible();
  await expect(tip).toContainText(" · ");
  await expect(details).toHaveCount(0);
  await page.waitForTimeout(500);
  expect((await chart.boundingBox())!.height).toBeCloseTo(full, 0);
  const same = (await lane.locator("rect").first().boundingBox())!;
  expect(same.y).toBeCloseTo(box.y, 0);
  const t = (await tip.boundingBox())!;
  expect(t.y).toBeGreaterThanOrEqual(0);
  expect(t.y + t.height).toBeLessThanOrEqual(height);
  // It takes no clicks and leaves with the pointer.
  await page.mouse.move(2, 2);
  await expect(tip).toHaveCount(0);
  // Click, however long the pointer rested first: the aimed bar, every time.
  await page.mouse.move(x - 3, y);
  await page.mouse.move(x, y);
  await page.waitForTimeout(500);
  await page.mouse.down();
  await page.mouse.up();
  await expect(details).toBeVisible();
  // A top-team selection splits the chart; each half draws every bar, cropped.
  const top = chart.first(),
    selected = top.locator(".ibpl-ribbon-lane.is-pair-selected");
  await expect(selected).toHaveCount(1);
  await expect(selected).toHaveAttribute("data-player-key", aimed);
  await expect(selected).toHaveAttribute("data-start", String(from));
  // The chart gave room to the lineups; all of it inside the window.
  await page.mouse.move(2, 2);
  const shrunk = await chart.evaluateAll((svgs) =>
    svgs.reduce((sum, s) => sum + s.getBoundingClientRect().height, 0),
  );
  expect(shrunk).toBeLessThan(full);
  const end = (await details.boundingBox())!;
  expect(end.y + end.height).toBeLessThanOrEqual(height);
  // No blank space below a short list: the box ends where its content does, and
  // the chart keeps what the lineups did not need.
  const fills = await details.evaluate((el) => {
    const box = el.getBoundingClientRect(),
      list = el.querySelector(".ibpl-ribbon-shared-lineups")!,
      card = el.querySelector(".ibpl-ribbon-detail.is-card"),
      rows = [...list.querySelectorAll(".ibpl-ribbon-shared-lineup")],
      last = Math.max(...rows.map((r) => r.getBoundingClientRect().bottom));
    return {
      blankUnderList: box.bottom - last,
      scrolls: list.scrollHeight > list.clientHeight + 1,
      cardContent: card ? card.scrollHeight : 0,
      height: box.height,
    };
  });
  if (!fills.scrolls)
    expect(
      Math.min(fills.blankUnderList, fills.height - fills.cardContent),
    ).toBeLessThanOrEqual(30);
  // Every lineup listed lies inside the clicked stint.
  const metas = await page
    .locator(".ibpl-ribbon-shared-lineup .ibpl-ribbon-detail-meta")
    .allInnerTexts();
  expect(metas.length).toBeGreaterThan(0);
  const secs = (c: string) => {
    const [m, s] = c.split(":").map(Number);
    return m * 60 + s;
  };
  for (const meta of metas)
    for (const r of meta.matchAll(/(\d+:\d\d)–(\d+:\d\d)/g)) {
      expect(secs(r[1])).toBeGreaterThanOrEqual(Math.floor(from));
      expect(secs(r[2])).toBeLessThanOrEqual(Math.ceil(to));
    }
  // Only that stint is broken into lineups on the chart.
  const broken = top.locator(".ibpl-ribbon-selection-overlay");
  await expect(broken).toHaveCount(1);
  // Another stint of the same player moves there instead of deselecting.
  const other = top
    .locator(`.ibpl-ribbon-lane[data-player-key="${aimed}"]`)
    .filter({ hasNot: page.locator(".ibpl-ribbon-selection-overlay") })
    .first();
  if (await other.count()) {
    const start = await other.getAttribute("data-start");
    await other.click();
    await page.mouse.move(2, 2);
    await expect(selected).toHaveAttribute("data-start", start!);
    await expect(page.locator(".ibpl-ribbon-pair-status")).toContainText(
      "in this stint",
    );
  }
  // The name still means the whole game: all the player's bars are broken up.
  await page.getByRole("button", { name: "Clear players" }).click();
  await expect(details).toHaveCount(0);
  await top
    .locator(".ibpl-ribbon-row")
    .filter({ has: page.locator(".ibpl-ribbon-name") })
    .nth(5)
    .click();
  await page.mouse.move(2, 2);
  await expect(page.locator(".ibpl-ribbon-pair-status")).not.toContainText(
    "in this stint",
  );
});
// The first look at the wide chart needs no scrolling (user 2026-11-04).
for (const [width, height] of [
  [1920, 1080],
  [1366, 768],
  [1280, 720],
] as const)
  test(`Gameflow fits a ${width}x${height} window`, async ({ page }, info) => {
    test.skip(info.project.name === "phone", "phones keep the quarter cards");
    const { g, answer, url } = loadGameflow("euroleague_overtime.json");
    await page.setViewportSize({ width, height });
    await page.route("**/api/v1/**", (route) =>
      route.fulfill({
        json: new URL(route.request().url()).pathname.endsWith("/version")
          ? { leagues: [{ league: g.case.league, version: "golden" }] }
          : answer,
      }),
    );
    await page.goto(url);
    const chart = page.locator("svg.ibpl-ribbon:not(.ibpl-gameflow-gutter)");
    await expect(chart).toHaveCount(1);
    // Every player's row is drawn, and the last one is inside the window.
    await expect
      .poll(async () => {
        const box = (await chart.boundingBox())!;
        return box.y + box.height;
      })
      .toBeLessThanOrEqual(height);
    const box = (await chart.boundingBox())!;
    const column = (await page.locator(".ibpl-gameflow-full").boundingBox())!;
    expect(box.width).toBeCloseTo(column.width, 0);
    await expect(page.locator(".ibpl-ribbon-row")).toHaveCount(
      new Set(answer.bars.map((b) => `${b.side}:${b.player_key}`)).size,
    );
  });
