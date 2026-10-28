import { test, expect } from "@playwright/test";
import { openNavMenu } from "./navMenu";
for (const [saved, league] of [
  ["E", "euroleague"],
  ["U", "eurocup"],
  ["il", "il"],
  ["bad", "il"],
])
  test(`[X-01] root restores saved league ${saved}`, async ({ page }) => {
    await page.addInitScript(
      (value) => localStorage.setItem("ibpl_league_select", value),
      saved,
    );
    await page.route("**/api/v1/**", (route) =>
      route.fulfill({
        json: { leagues: [], rows: [], teams: [], meta: { version: "test" } },
      }),
    );
    await page.goto("/");
    await expect(page).toHaveURL(new RegExp(`/${league}/home$`));
    await expect(page.locator(".home-nav-card")).toHaveCount(
      league === "il" ? 6 : 5,
    );
    // Explicit league URLs retain their meaning even if the preference differs.
    await page.goto("/il/home?team_id=all");
    await expect(page).toHaveURL(/\/il\/home\?team_id=all$/);
  });
for (const next of ["euroleague", "eurocup"] as const)
  test(`[X-01] Compare switches to ${next} Home without carrying Israeli IDs`, async ({
    page,
  }) => {
    const errors: string[] = [];
    page.on("pageerror", (e) => errors.push(e.message));
    await page.route("**/api/v1/**", (route) => {
      const u = new URL(route.request().url());
      return route.fulfill({
        json: u.pathname.endsWith("/version")
          ? { leagues: [] }
          : {
              rows: [],
              teams: [],
              players: [],
              rounds: [],
              meta: { version: "test", key: u.searchParams.toString() },
            },
      });
    });
    await page.goto(
      "/il/compare?season=2026&preset=home_away&a_teams=2&b_teams=5",
    );
    // Wait for client data and responsive layout to settle before opening
    // the navbar control; its initial server-rendered position can move.
    await expect(
      page.getByText("No comparisons match the current filters.", {
        exact: true,
      }),
    ).toBeVisible();
    if (page.viewportSize()!.width < 1000)
      await page.getByRole("button", { name: "Toggle navigation" }).click();
    await openNavMenu(page, /^League:/);
    await page
      .locator("#league_menu")
      .getByRole("link", {
        name: next === "euroleague" ? "EuroLeague" : "EuroCup",
        exact: true,
      })
      .click();
    await expect(page).toHaveURL(new RegExp(`/${next}/home$`));
    await expect(
      // includeHidden also counts the aria-hidden icon glyph in the name, so
      // match the label at the end rather than the whole name.
      page.getByRole("tab", { name: /(^|\s)Home$/, includeHidden: true }),
    ).toHaveAttribute("aria-selected", "true");
    await expect(page.getByRole("tab", { name: /Compare/ })).toHaveCount(0);
    await expect(page.locator(".home-nav-card")).toHaveCount(5);
    expect(
      await page.evaluate(() => localStorage.getItem("ibpl_league_select")),
    ).toBe(next === "euroleague" ? "E" : "U");
    expect(
      await page.evaluate(
        () => document.documentElement.scrollWidth <= innerWidth + 1,
      ),
    ).toBe(true);
    expect(errors).toEqual([]);
  });
