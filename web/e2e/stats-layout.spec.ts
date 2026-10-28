// The shared stats-tab layout (design 2026-10-28), proven on On/Off with the
// stored Shiny answers: filters drawer, steppers, view tabs, key, columns.
import { test, expect, type Page } from "@playwright/test";
import { loadGolden } from "../test/onoffGoldens";

async function stub(page: Page, league = "il") {
  const requests: URL[] = [];
  await page.route("**/api/v1/**", (route) => {
    const url = new URL(route.request().url());
    const view = url.searchParams.get("view") ?? "summary";
    const { answer } = loadGolden(`${league}_${view}_default.json`);
    if (!url.pathname.endsWith("/version")) requests.push(url);
    return route.fulfill({
      json: url.pathname.endsWith("/version")
        ? { leagues: [{ league, version: answer.meta.version }] }
        : answer,
    });
  });
  const { g } = loadGolden(`${league}_summary_default.json`);
  await page.goto(`/${league}/onoff?${new URLSearchParams(g.query)}`);
  await expect(page.locator(".ibpl-dt-scroll tbody tr").first()).toBeVisible();
  return { g, requests };
}
const drawer = (page: Page) => page.getByRole("dialog", { name: "Filters" });
const filtersButton = (page: Page) =>
  page.locator(".chips-drawer-toggle");
async function pickTeam(page: Page, name: string) {
  const teams = page.getByRole("combobox", { name: "Teams", exact: true });
  await teams.fill(name);
  await teams.press("ArrowDown");
  await teams.press("Enter");
  await teams.press("Escape");
  await expect(page).toHaveURL(/teams=/);
}

test("drawer opens, pushes content on desktop, closes on Escape", async ({
  page,
}, info) => {
  await stub(page);
  await expect(page.locator(".ibpl-filter-col")).toHaveCount(0);
  await expect(drawer(page)).toBeHidden();
  await filtersButton(page).click();
  await expect(drawer(page)).toBeVisible();
  if (info.project.name === "desktop") {
    const main = (await page.locator('[role="main"]').boundingBox())!;
    const box = (await drawer(page).boundingBox())!;
    expect(main.x + main.width).toBeLessThanOrEqual(box.x + 1);
  } else {
    const box = (await drawer(page).boundingBox())!;
    expect(box.width).toBe(page.viewportSize()!.width);
  }
  await page.keyboard.press("Escape");
  await expect(drawer(page)).toBeHidden();
  await expect(filtersButton(page)).toBeFocused();
});

test("phone drawer closes from its result button", async ({ page }, info) => {
  test.skip(info.project.name !== "phone", "phone layout only");
  await stub(page);
  await filtersButton(page).click();
  await drawer(page)
    .getByRole("button", { name: /^Show \d+ players$/ })
    .click();
  await expect(drawer(page)).toBeHidden();
});

test("Clear all filters shows only when a filter is set", async ({ page }) => {
  const { g } = await stub(page);
  await filtersButton(page).click();
  const clear = drawer(page).getByRole("button", { name: "Clear all filters" });
  await expect(clear).toHaveCount(0);
  await pickTeam(page, String(g.expected.data[0].Team));
  await expect(filtersButton(page)).toHaveText(/Filters · 1/);
  await clear.click();
  await expect(page).not.toHaveURL(/teams=/);
  await expect(clear).toHaveCount(0);
});

test("a chip's edit pencil opens the drawer at its control", async ({
  page,
}) => {
  const { g } = await stub(page);
  await filtersButton(page).click();
  await pickTeam(page, String(g.expected.data[0].Team));
  await page.keyboard.press("Escape");
  await expect(drawer(page)).toBeHidden();
  await page.locator(".filter-chip .chip-edit").first().click();
  await expect(drawer(page)).toBeVisible();
  await expect(
    page.getByRole("combobox", { name: "Teams", exact: true }),
  ).toBeFocused();
});
