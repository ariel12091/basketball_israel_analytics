import { test, expect } from "@playwright/test";
import { loadLineup, pageAnswer } from "../test/lineupGoldens";
for (const league of ["il", "euroleague"] as const)
  for (const view of ["summary", "four_factors"] as const)
    test(`Lineup modal ${league} ${view}: viewport and keyboard focus`, async ({
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
      await page.locator("button.ibpl-lineup-link").first().click();
      const dialog = page.getByRole("dialog"),
        dismiss = dialog.getByRole("button", { name: "Dismiss", exact: true });
      await expect(dialog.locator("tbody tr").first()).toBeVisible();
      const header = (await dialog.locator(".modal-header").boundingBox())!,
        button = (await dismiss.boundingBox())!,
        viewport = page.viewportSize()!;
      expect(header.y).toBeGreaterThanOrEqual(0);
      expect(header.y + header.height).toBeLessThanOrEqual(viewport.height);
      expect(button.y + button.height).toBeLessThanOrEqual(viewport.height);
      expect(button.x + button.width).toBeLessThanOrEqual(viewport.width);
      await expect(dismiss).toBeFocused();
      await page.keyboard.press("Tab");
      expect(
        await dialog.evaluate((el) => el.contains(document.activeElement)),
      ).toBe(true);
      await page.screenshot({
        path: `../output/playwright/lineup-modal-layout-${league}-${view}-${info.project.name}.png`,
        fullPage: true,
      });
      await dismiss.click();
      await expect(dialog).toHaveCount(0);
      expect(
        await page.evaluate(
          () => document.documentElement.scrollWidth <= innerWidth,
        ),
      ).toBe(true);
    });
