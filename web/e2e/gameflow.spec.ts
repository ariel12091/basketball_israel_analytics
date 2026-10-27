import { test, expect } from "@playwright/test";
import { gameflowFiles, loadGameflow } from "../test/gameflowGoldens";
import {
  windowDetail,
  sharedWindows,
  clock,
  laneKey,
} from "../src/rules/gameflow";
for (const file of gameflowFiles)
  test(`Gameflow ${file}`, async ({ page }, info) => {
    const { g, answer, url } = loadGameflow(file),
      errors: string[] = [];
    page.on("pageerror", (e) => errors.push(e.message));
    await page.route("**/api/v1/**", (route) =>
      route.fulfill({
        json: new URL(route.request().url()).pathname.endsWith("/version")
          ? { leagues: [{ league: g.case.league, version: "golden" }] }
          : answer,
      }),
    );
    await page.goto(url);
    const charts = page.locator("svg.ibpl-ribbon:not(.ibpl-gameflow-gutter)");
    const phone = info.project.name === "phone";
    await expect(charts).toHaveCount(phone ? answer.bounds.length : 1);
    for (let i = 0; i < (phone ? answer.bounds.length : 1); i++) {
      const start = phone && i ? answer.bounds[i - 1] : 0,
        end = phone ? answer.bounds[i] : answer.bounds.at(-1)!;
      const bars = answer.bars.filter(
        (b) => b.end_elapsed > start && b.start_elapsed < end,
      );
      const labels = await charts
        .nth(i)
        .locator(".ibpl-ribbon-lane")
        .evaluateAll((nodes) => nodes.map((n) => n.getAttribute("aria-label")));
      expect(labels).toEqual(
        bars.map(
          (b) =>
            windowDetail(
              answer.lanes,
              answer.steps,
              answer.dict,
              b,
              Math.max(start, b.start_elapsed),
              Math.min(end, b.end_elapsed),
            ).label,
        ),
      );
      if (phone)
        await expect(charts.nth(i).locator(".ibpl-ribbon-pm")).toHaveCount(0);
    }
    if (answer.health)
      await expect(page.getByRole("note")).toHaveText(answer.health);
    expect(
      await page.evaluate(
        () => document.documentElement.scrollWidth <= innerWidth + 1,
      ),
    ).toBe(true);
    if (file === "il_regular.json" || file === "euroleague_overtime.json")
      await page.screenshot({
        path: `../output/playwright/gameflow-${file}-${info.project.name}.png`,
        fullPage: true,
      });
    expect(errors).toEqual([]);
  });

for (const league of ["il", "euroleague", "eurocup"] as const)
  test(`Gameflow selection, lineup focus and score ${league}`, async ({
    page,
  }, info) => {
    const { g, answer, url } = loadGameflow(`${league}_regular.json`);
    await page.route("**/api/v1/**", (r) =>
      r.fulfill({
        json: new URL(r.request().url()).pathname.endsWith("/version")
          ? { leagues: [{ league, version: "golden" }] }
          : answer,
      }),
    );
    await page.goto(url);
    const chart = page
      .locator("svg.ibpl-ribbon:not(.ibpl-gameflow-gutter)")
      .first();
    const starters = answer.bars.filter((b) => b.start_elapsed === 0);
    const a = starters[0],
      b = starters.find((b) => b.side !== a.side)!;
    const pick = async (key: string) => {
      const p = chart
        .locator(`.ibpl-ribbon-lane[data-player-key="${key}"]`)
        .first();
      await p.click();
      await page.mouse.move(0, 0);
    };
    await pick(laneKey(a));
    await expect(
      page.getByRole("region", { name: "Stint lineups", exact: true }),
    ).toBeVisible();
    await pick(laneKey(b));
    const windows = sharedWindows(answer.bars, [laneKey(a), laneKey(b)]),
      secs = windows.reduce((s, w) => s + w.end - w.start, 0);
    await expect(page.locator(".ibpl-ribbon-pair-status")).toContainText(
      `${clock(secs)} on court together`,
    );
    const groups = page.getByRole("region", {
      name: "Shared lineups",
      exact: true,
    });
    await expect(groups).toBeVisible();
    await expect(groups.locator(".ibpl-ribbon-detail-side")).toHaveCount(0);
    await groups.getByRole("button").first().click();
    await expect(groups.getByRole("button").first()).toHaveAttribute(
      "aria-pressed",
      "true",
    );
    await expect(
      page.locator(".ibpl-ribbon-lineup-mark").first(),
    ).toBeAttached();
    await page.getByRole("button", { name: "Clear players" }).click();
    await expect(page.locator(".ibpl-ribbon-pair-status")).toContainText(
      "Select two or more",
    );
    const hit = chart.locator(".ibpl-ribbon-margin-hit");
    await hit.hover();
    await expect(page.getByRole("tooltip")).toContainText(g.case.team_name);
    if (info.project.name === "phone") {
      await page
        .getByRole("button", { name: "View full timeline", exact: true })
        .click();
      const scroller = page.locator(".ibpl-gameflow-full");
      const pin = page.locator(".ibpl-gameflow-gutter"),
        x = (await pin.boundingBox())!.x;
      await scroller.evaluate((el) => (el.scrollLeft = 350));
      expect((await pin.boundingBox())!.x).toBeCloseTo(x, 0);
      await pin.locator(".ibpl-ribbon-name").first().press("Enter");
      await expect(page.locator(".ibpl-ribbon-pair-status")).not.toContainText(
        "Select two or more",
      );
      await page.getByRole("button", { name: "Jump to Q2" }).click();
      await expect(page.locator(".ibpl-ribbon-quarter-card")).toHaveCount(
        answer.bounds.length,
      );
    }
  });

test("Gameflow errors can be retried", async ({ page }) => {
  const { answer, url } = loadGameflow("il_regular.json");
  let failed = true;
  await page.route("**/api/v1/**", (r) =>
    !new URL(r.request().url()).pathname.endsWith("/gameflow")
      ? r.fulfill({ json: { leagues: [] } })
      : failed
        ? r.fulfill({
            status: 503,
            json: { error: "Temporarily unavailable" },
          })
        : r.fulfill({ json: answer }),
  );
  await page.goto(url);
  await expect(
    page.getByRole("alert").filter({ hasText: "Temporarily unavailable" }),
  ).toBeVisible();
  failed = false;
  await page.getByRole("button", { name: "Retry", exact: true }).click();
  await expect(page.locator("svg.ibpl-ribbon").first()).toBeVisible();
});
