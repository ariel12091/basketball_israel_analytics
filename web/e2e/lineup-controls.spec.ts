import { test, expect, type Page } from "@playwright/test";
import { loadLineup, pageAnswer } from "../test/lineupGoldens";
/** Opens the filters panel (the Filters button at the toolbar's right end). */
async function openFilters(page: Page) {
  const button = page.locator(".chips-drawer-toggle");
  if ((await button.getAttribute("aria-expanded")) !== "true")
    await button.click();
  await expect(page.getByRole("dialog", { name: "Filters" })).toBeVisible();
}
const clearAll = (page: Page) =>
  page
    .getByRole("dialog", { name: "Filters" })
    .getByRole("button", { name: "Clear all filters", exact: true })
    .click();

for (const league of ["il", "euroleague", "eurocup"] as const)
  test(`Lineup Summary ${league}: table search, column ranges and page size`, async ({
    page,
  }, info) => {
    const { g, answer, roster } = loadLineup(`${league}_summary_default.json`);
    await page.route("**/api/v1/**", (route) => {
      const path = new URL(route.request().url()).pathname;
      return route.fulfill({
        json: path.endsWith("/version")
          ? { leagues: [{ league, version: answer.meta.version }] }
          : path.endsWith("/lineup-roster")
            ? roster
            : pageAnswer(answer, route.request().url()),
      });
    });
    await page.goto(`/${league}/lineups?season=${g.query.season}`);
    await expect(
      page.locator(".ibpl-dt-scroll tbody tr").first(),
    ).toContainText("TOTAL");
    const minposs = page.locator(
      league === "il" ? "#ld_minposs" : "#euro_ld_minposs",
    );
    await page.locator(".paginate_button.next").click();
    await expect(page.locator(".dataTables_info")).toContainText("Showing 51");
    await openFilters(page);
    const team = page.getByRole("combobox", { name: "Team", exact: true });
    await team.fill(String(answer.rows[0].Team).slice(0, 6));
    await team.press("ArrowDown");
    await team.press("Enter");
    await team.press("Escape");
    await expect(page.locator(".dataTables_info")).toContainText("Showing 1");
    await clearAll(page);
    await page.keyboard.press("Escape");
    // Reset may re-arm the auto minimum, whose recalculation redraws the
    // table a beat later; pick the page size until it holds.
    await expect(async () => {
      await page
        .getByLabel("Entries per page", { exact: true })
        .selectOption("1000");
      expect(
        await page.locator(".ibpl-dt-scroll tbody tr").count(),
      ).toBeGreaterThan(50);
    }).toPass();
    const search = page.getByRole("textbox", {
      name: "Search table",
      exact: true,
    });
    await search.fill("no-such-lineup-xyz");
    await expect(page.locator(".dataTables_info")).toContainText(
      "0 of 0 entries",
    );
    await search.fill("TOTAL");
    await expect(page.locator(".ibpl-dt-scroll tbody tr")).toHaveCount(1);
    await search.fill("");
    const column = page.locator(
      'thead th:has(input[aria-label="Min minimum"])',
    );
    await column.locator("summary").click();
    await page
      .getByRole("spinbutton", { name: "Min minimum", exact: true })
      .fill("100");
    await expect(page.locator(".dataTables_info")).not.toContainText(
      "0 of 0 entries",
    );
    const values = await page
      .locator('tbody td[data-column="minutes"]')
      .evaluateAll((tds) =>
        tds.map((td) => parseFloat(td.textContent!.replaceAll(",", ""))),
      );
    expect(values.every((v) => v >= 100)).toBe(true);
    expect(
      await page.evaluate(
        () => document.documentElement.scrollWidth <= innerWidth,
      ),
    ).toBe(true);
  });
