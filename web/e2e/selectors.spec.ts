import { test, expect } from "@playwright/test";
const teams = [
  { id: 701, name: "Hapoel Tel-Aviv" },
  { id: 702, name: "AS Monaco" },
  { id: 703, name: "Real Madrid" },
];
for (const league of ["il", "euroleague", "eurocup"]) {
  test(`${league}: searchable team/opponent controls, named chips and grouped headers`, async ({
    page,
  }, info) => {
    await page.route("**/api/v1/**", (route) =>
      route.fulfill({
        json: route.request().url().includes("/version")
          ? { leagues: [] }
          : {
              rows: [],
              meta: {
                version: "test",
                teams,
                rounds: [1, 2],
                phases: [],
                shot_averages: null,
              },
            },
      }),
    );
    await page.goto(
      `/${league}/game-logs?season=${league === "il" ? 2026 : 2025}`,
    );
    await expect(page.getByRole("status")).toHaveText("0 team-games");
    if (info.project.name === "phone")
      await page
        .getByRole("button", { name: "Show Filters", exact: true })
        .click();
    const teamLabel = league === "il" ? "Team" : "Teams";
    const picker = page.getByRole("combobox", { name: teamLabel, exact: true });
    await picker.fill("monaco");
    await expect(
      page.getByRole("option", { name: "AS Monaco", exact: true }),
    ).toBeVisible();
    await expect(
      page.getByRole("option", { name: "Real Madrid", exact: true }),
    ).toHaveCount(0);
    await picker.press("ArrowDown");
    await picker.press("Enter");
    const key = league === "il" ? "team" : "teams";
    await expect
      .poll(() => new URL(page.url()).searchParams.get(key))
      .toBe("702");
    await expect(
      page.getByRole("button", { name: `Clear ${key}`, exact: true }),
    ).toContainText("AS Monaco");
    await expect(
      page.getByRole("button", { name: `Clear ${key}`, exact: true }),
    ).not.toContainText("702");
    if (league !== "il") {
      await picker.fill("madrid");
      await page
        .getByRole("option", { name: "Real Madrid", exact: true })
        .click();
      await expect
        .poll(() => new URL(page.url()).searchParams.get("teams"))
        .toBe("702,703");
      await expect(
        page.getByRole("button", { name: "Clear teams", exact: true }),
      ).toContainText("AS Monaco, Real Madrid");
    }
    const opponents = page.getByRole("combobox", {
      name: "Opponents",
      exact: true,
    });
    await opponents.fill("hapoel");
    await page
      .getByRole("option", { name: "Hapoel Tel-Aviv", exact: true })
      .click();
    await opponents.fill("madrid");
    await opponents.press("Enter");
    await expect
      .poll(() => new URL(page.url()).searchParams.get("opponents"))
      .toBe("701,703");
    const chip = page.getByRole("button", {
      name: "Clear opponents",
      exact: true,
    });
    await expect(chip).toContainText("Hapoel Tel-Aviv, Real Madrid");
    await expect(chip).not.toContainText("701");
    await opponents.press("Escape");
    expect(
      await page.evaluate(
        () => document.documentElement.scrollWidth <= innerWidth,
      ),
    ).toBe(true);
    await page
      .getByRole("button", { name: "Four Factors", exact: true })
      .click();
    for (const [name, span] of [
      ["Offense", "5"],
      ["Defense", "5"],
      ["Usage", "2"],
    ])
      await expect(
        page
          .locator('thead th[scope="colgroup"]')
          .filter({ hasText: new RegExp(`^${name}$`) }),
      ).toHaveAttribute("colspan", span);
    await page.reload();
    await expect(
      page.getByRole("button", { name: `Clear ${key}`, exact: true }),
    ).toContainText("AS Monaco");
    await expect(
      page.getByRole("button", { name: "Clear opponents", exact: true }),
    ).toContainText("Hapoel Tel-Aviv, Real Madrid");
    if (info.project.name === "phone")
      await page
        .getByRole("button", { name: "Show Filters", exact: true })
        .click();
    await page
      .getByRole("button", {
        name: "Remove Hapoel Tel-Aviv from Opponents",
        exact: true,
      })
      .click();
    await expect
      .poll(() => new URL(page.url()).searchParams.get("opponents"))
      .toBe("703");
    await page
      .getByRole("button", { name: "Clear opponents", exact: true })
      .click();
    await expect
      .poll(() => new URL(page.url()).searchParams.get("opponents"))
      .toBe(null);
    await expect(
      page.getByRole("combobox", { name: "Opponents", exact: true }),
    ).toHaveValue("");
    await page
      .getByRole("combobox", { name: teamLabel, exact: true })
      .fill("no such team");
    await expect(
      page.getByText("No matching teams", { exact: true }),
    ).toBeVisible();
    await page
      .getByRole("combobox", { name: teamLabel, exact: true })
      .press("Escape");
    await expect(
      page.getByRole("combobox", { name: teamLabel, exact: true }),
    ).toHaveValue("");
    await page.screenshot({
      path: `../output/playwright/selectors-${league}-${info.project.name}.png`,
      fullPage: true,
    });
  });
}
