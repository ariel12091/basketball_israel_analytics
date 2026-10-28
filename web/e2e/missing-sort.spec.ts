import { test, expect } from "@playwright/test";
import { loadTeam } from "../test/teamGoldens";

for (const league of ["il", "euroleague", "eurocup"])
  test(`[X-13][TR-06][league:${league}] missing ranked ratings stay last in either direction`, async ({
    page,
  }) => {
    const { answer } = loadTeam(`${league}_summary_default.json`);
    answer.rows = [null, 0, -5, 10].map((value, i) => ({
      ...answer.rows[0],
      team_id: i + 1,
      team_name: `Team ${i + 1}`,
      def_ppp: value,
    }));
    await page.route("**/api/v1/**", (route) =>
      route.fulfill({
        json: route.request().url().includes("/version")
          ? { leagues: [] }
          : answer,
      }),
    );
    await page.goto(
      `/${league}/team-ratings?season=${league === "il" ? "2026" : "2025"}`,
    );
    const cells = page.locator('td[data-column="def_ppp"]');
    await expect(cells).toHaveCount(4);
    const header = page.locator('thead th[data-column="def_ppp"]');
    await header.click({ position: { x: 6, y: 6 } });
    await expect(cells.last()).toContainText("NA");
    await expect(cells.first()).toContainText("-5.0");
    await header.click({ position: { x: 6, y: 6 } });
    await expect(cells.last()).toContainText("NA");
    await expect(cells.first()).toContainText("10.0");
  });

for (const league of ["il", "euroleague", "eurocup"])
  test(`[X-13][league:${league}] missing metrics stay last ascending and descending`, async ({
    page,
  }) => {
    await page.route("**/api/v1/**", (route) =>
      route.fulfill({
        json: route.request().url().includes("/version")
          ? { leagues: [] }
          : {
              rows: [null, 0, -5, 10].map((value, i) => ({
                game_id: i + 1,
                team_id: 2,
                team: "Test",
                opponent: "Opponent",
                game_date: "2026-01-01",
                gn: i + 1,
                round_number: i + 1,
                off_ppp: value,
                def_ppp: value,
                off_poss: 50,
                def_poss: 50,
              })),
              meta: {
                version: "test",
                teams: [],
                rounds: [],
                phases: [],
                shot_averages: null,
              },
            },
      }),
    );
    await page.goto(
      `/${league}/game-logs?season=${league === "il" ? "2026" : "2025"}`,
    );
    const cells = page.locator('td[data-column="def_ppp"]');
    await expect(cells).toHaveCount(4);
    const header = page.locator('thead th[data-column="def_ppp"]');
    await header.click({ position: { x: 6, y: 6 } });
    await expect(cells).toHaveText(["-5.0", "0.0", "10.0", ""]);
    await header.click({ position: { x: 6, y: 6 } });
    await expect(cells).toHaveText(["10.0", "0.0", "-5.0", ""]);
    expect(
      await page.evaluate(
        () => document.documentElement.scrollWidth <= innerWidth + 1,
      ),
    ).toBe(true);
  });
