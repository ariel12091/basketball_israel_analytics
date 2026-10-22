import { test, expect } from "@playwright/test";
import { loadGameflow } from "../test/gameflowGoldens";
test.skip(
  process.env.LIVE_DB_SMOKE !== "1",
  "Opt-in live read-only SSR checks",
);
test.use({ extraHTTPHeaders: {} });
for (const league of ["il", "euroleague", "eurocup"] as const)
  test(`Gameflow initial server HTML ${league}`, async ({ page }) => {
    const { url, answer } = loadGameflow(`${league}_regular.json`);
    const response = await page.goto(url);
    const html = await response!.text();
    expect(html).toContain('class="ibpl-ribbon');
    expect(html).toContain(answer.meta.ownTeam);
    await expect(page.locator("svg.ibpl-ribbon").first()).toBeVisible();
    await expect(page.locator(".ibpl-ribbon-pair-status")).toContainText(
      "Select two or more",
    );
    await expect(
      page.getByRole("link", { name: "Back to Game Logs" }),
    ).toHaveAttribute(
      "href",
      `/${league}/game-logs?season=${answer.meta.season}`,
    );
    expect(
      await page.evaluate(
        () => document.documentElement.scrollWidth <= innerWidth + 1,
      ),
    ).toBe(true);
  });
