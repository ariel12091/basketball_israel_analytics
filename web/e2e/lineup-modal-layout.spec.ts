// A lineup's game log opens in place beneath its row (user 2026-10-28: no
// modal windows): visible without scrolling sideways, closable from its own
// Close button, and a second click on the lineup folds it away.
import { test, expect } from "@playwright/test";
import { loadLineup, pageAnswer } from "../test/lineupGoldens";
for (const league of ["il", "euroleague", "eurocup"] as const)
  for (const view of ["summary", "four_factors"] as const)
    test(`Lineup game log ${league} ${view}: opens in place under its row`, async ({
      page,
    }, info) => {
      const { g, answer, roster, games } = loadLineup(
        `${league}_${view}_default.json`,
      );
      await page.route("**/api/v1/**", (route) => {
        const path = new URL(route.request().url()).pathname;
        return route.fulfill({
          json: path.endsWith("/version")
            ? { leagues: [{ league, version: answer.meta.version }] }
            : path.endsWith("/lineup-roster")
              ? roster
              : path.endsWith("/lineup-games")
                ? games
                : pageAnswer(answer, route.request().url()),
        });
      });
      await page.goto(
        `/${league}/lineups?season=${g.query.season}&view=${view}`,
      );
      const link = page.locator("button.ibpl-lineup-link").first();
      await link.click();
      await expect(page.getByRole("dialog")).toHaveCount(0);
      const detail = page.locator("tr.ibpl-row-detail");
      await expect(detail).toHaveCount(1);
      // Directly beneath the clicked lineup's row.
      const name = (await link.textContent())!;
      expect(
        await detail.evaluate(
          (tr) => tr.previousElementSibling?.textContent ?? "",
        ),
      ).toContain(name);
      const log = detail.locator(".ibpl-lineup-games");
      await expect(log.locator("tbody tr").first()).toBeVisible();
      // Its header and Close sit inside the visible width.
      const close = log.getByRole("button", { name: "Close game log" });
      const box = (await close.boundingBox())!,
        viewport = page.viewportSize()!;
      expect(box.x + box.width).toBeLessThanOrEqual(viewport.width);
      await page.screenshot({
        path: `../output/playwright/lineup-game-log-${league}-${view}-${info.project.name}.png`,
        fullPage: true,
      });
      await close.click();
      await expect(page.locator(".ibpl-row-detail")).toHaveCount(0);
      // A second click on the same lineup toggles it.
      await link.click();
      await expect(page.locator(".ibpl-row-detail")).toHaveCount(1);
      await link.click();
      await expect(page.locator(".ibpl-row-detail")).toHaveCount(0);
      expect(
        await page.evaluate(
          () => document.documentElement.scrollWidth <= innerWidth,
        ),
      ).toBe(true);
    });
