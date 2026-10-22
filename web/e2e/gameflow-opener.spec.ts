import { test, expect } from "@playwright/test";
import { readFileSync } from "node:fs";
import { gameLogs } from "../src/rules/gameLogs";
import { loadGameflow } from "../test/gameflowGoldens";
for (const league of ["il", "euroleague", "eurocup"] as const)
  for (const view of ["summary", "four_factors"] as const)
    test(`Game Logs opens and closes Gameflow ${league} ${view}`, async ({
      page,
    }, info) => {
      const g = JSON.parse(
        readFileSync(
          `test/goldens/game_logs/${league === "il" ? "il" : "el"}_${view === "summary" ? "summary" : "ff"}_default.json`,
          "utf8",
        ),
      );
      const result = gameLogs(g.input, g.input.scoreless ?? [], g.params);
      const { answer } = loadGameflow(`${league}_regular.json`);
      await page.route("**/api/v1/**", (r) => {
        const p = new URL(r.request().url()).pathname;
        return r.fulfill({
          json: p.endsWith("/version")
            ? { leagues: [] }
            : p.endsWith("/gameflow")
              ? answer
              : {
                  ...result,
                  meta: {
                    ...result.meta,
                    version: "golden",
                    teams: [],
                    rounds: [],
                    phases: [],
                    view,
                  },
                },
        });
      });
      await page.goto(
        `/${league}/game-logs?season=${league === "il" ? 2026 : 2025}&view=${view}`,
      );
      const link = page.locator('td[data-column="gameflow"] a').first();
      await expect(link).toHaveAttribute(
        "href",
        new RegExp(`^/${league}/gameflow\\?game_id=\\d+&team_id=\\d+$`),
      );
      const href = await link.getAttribute("href");
      await link.click();
      const panel =
        info.project.name === "phone"
          ? page.getByRole("region", { name: "Gameflow", exact: true })
          : page.getByRole("dialog", {
              name: `${answer.meta.ownTeam} vs ${answer.meta.oppTeam}`,
              exact: true,
            });
      await expect(panel.locator("svg.ibpl-ribbon").first()).toBeVisible();
      if (info.project.name === "desktop") {
        await expect(
          page.getByRole("button", { name: "Close gameflow", exact: true }),
        ).toBeFocused();
        await page.keyboard.press("Escape");
      } else
        await panel
          .getByRole("button", { name: "Close gameflow", exact: true })
          .click();
      await expect(panel).toHaveCount(0);
      await expect(link).toBeFocused();
      await link.click();
      await expect(panel.locator("svg.ibpl-ribbon").first()).toBeVisible();
      await panel
        .getByRole("button", { name: "Close gameflow", exact: true })
        .click();
      await expect(panel).toHaveCount(0);
      await page.goto(href!);
      await expect(page.locator("svg.ibpl-ribbon").first()).toBeVisible();
    });

test("Gameflow follows viewport width at tablet size", async ({ page }) => {
  const { answer, url } = loadGameflow("euroleague_regular.json");
  await page.route("**/api/v1/**", (r) =>
    r.fulfill({
      json: new URL(r.request().url()).pathname.endsWith("/version")
        ? { leagues: [] }
        : answer,
    }),
  );
  await page.setViewportSize({ width: 768, height: 1024 });
  await page.goto(url);
  await expect(page.locator(".ibpl-ribbon-quarter-card")).toHaveCount(4);
  await page.setViewportSize({ width: 1100, height: 900 });
  await expect(page.locator(".ibpl-ribbon-quarter-card")).toHaveCount(0);
  await expect(page.locator("svg.ibpl-ribbon")).toHaveCount(1);
});

test("Gameflow empty segments are explained", async ({ page }) => {
  const { answer, url } = loadGameflow("il_regular.json");
  await page.route("**/api/v1/**", (r) =>
    r.fulfill({
      json: new URL(r.request().url()).pathname.endsWith("/version")
        ? { leagues: [] }
        : { ...answer, lanes: [] },
    }),
  );
  await page.goto(url);
  await expect(
    page.getByText("This game has no segment data to draw.", { exact: true }),
  ).toBeVisible();
});
