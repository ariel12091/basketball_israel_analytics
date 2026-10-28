import { test, expect } from "@playwright/test";
import { loadGolden } from "../test/onoffGoldens";

for (const league of ["il", "euroleague", "eurocup"] as const) {
  test(`On/Off ${league}: Player Stats failure and removal`, async ({
    page,
  }) => {
    const { g, answer } = loadGolden(`${league}_summary_default.json`);
    const psRequests: string[] = [];
    await page.route("**/api/v1/**", (route) => {
      const url = route.request().url();
      if (url.includes("/player-stat-filters")) {
        psRequests.push(url);
        return route.fulfill({
          status: 503,
          json: { error: "Player Stats unavailable" },
        });
      }
      return route.fulfill({
        json: url.includes("/version")
          ? { leagues: [{ league, version: answer.meta.version }] }
          : answer,
      });
    });
    await page.goto(
      `/${league}/onoff?${new URLSearchParams({
        ...g.query,
        own_starters: "gte:3",
        stats: JSON.stringify([{ col: "ps_pts_pg", op: "ge", value: 15 }]),
      })}`,
    );
    await expect(
      page.getByText(
        "Player Stats filters could not be evaluated. Remove them or try again.",
      ),
    ).toBeVisible();
    expect(psRequests.length).toBeGreaterThan(0);
    expect(new URL(psRequests[0]).searchParams.has("own_starters")).toBe(false);
    await page
      .getByRole("link", { name: "Remove PTS/G filter", exact: true })
      .click();
    await expect(
      page.locator('.ibpl-dt-scroll tbody td[data-column="Player"]').first(),
    ).toBeVisible();
    await expect(page).not.toHaveURL(/stats=/);
  });

  test(`On/Off ${league}: route error and retry`, async ({ page }) => {
    const { g, answer } = loadGolden(`${league}_summary_default.json`);
    let attempts = 0;
    await page.route("**/api/v1/**", (route) => {
      if (route.request().url().includes("/version"))
        return route.fulfill({ json: { leagues: [] } });
      attempts++;
      return attempts === 1
        ? route.fulfill({ status: 503, json: { error: "Busy" } })
        : route.fulfill({ json: answer });
    });
    await page.goto(`/${league}/onoff?${new URLSearchParams(g.query)}`);
    await expect(
      page.getByRole("alert").filter({ hasText: "Busy" }),
    ).toBeVisible();
    await page.getByRole("button", { name: "Retry", exact: true }).click();
    await expect(
      page.locator('.ibpl-dt-scroll tbody td[data-column="Player"]').first(),
    ).toBeVisible();
    expect(attempts).toBe(2);
  });
}
