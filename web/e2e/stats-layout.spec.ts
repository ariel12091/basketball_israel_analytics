// The shared stats-tab layout (design 2026-10-28), proven on On/Off with the
// stored Shiny answers: filters drawer, steppers, view tabs, key, columns.
import { test, expect, type Page } from "@playwright/test";
import { loadGolden } from "../test/onoffGoldens";
import ui from "../src/data/shinyUi.json";
import { autoMinOn, possCols } from "../src/rules/onoff";

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
const filtersButton = (page: Page) => page.locator(".chips-drawer-toggle");
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
    // Wait for the slide-in to settle, then nothing of the page is covered.
    await expect
      .poll(async () => {
        const main = (await page.locator('[role="main"]').boundingBox())!;
        const box = (await drawer(page).boundingBox())!;
        return box.x + 1 - (main.x + main.width);
      })
      .toBeGreaterThanOrEqual(0);
  } else {
    const box = (await drawer(page).boundingBox())!;
    expect(box.width).toBeCloseTo(page.viewportSize()!.width, 0);
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

test("the key shows only what applies, next to the table", async ({ page }) => {
  await stub(page);
  const key = page.locator(".table-key");
  await expect(key).toContainText("Shading = league rank");
  await expect(key).toContainText("Shot cells:");
  await expect(page.locator(".legend-box")).toHaveCount(0);
  await expect(page.locator(".explainer-card")).toHaveCount(0);
  await page.getByRole("tab", { name: "Four Factors" }).click();
  await expect(page).toHaveURL(/view=four_factors/);
  await expect(key).toContainText("Shading = league rank");
  await expect(key).not.toContainText("Shot cells:");
});

test("About this view holds the view's explainer", async ({ page }) => {
  await stub(page);
  const explainer = (
    ui.onoff.il.views.summary as { kind: string; intro?: { t?: string }[] }[]
  ).find((b) => b.kind === "explainer")!;
  const first = String(explainer.intro![0].t ?? "").slice(0, 30);
  await page.getByRole("button", { name: "About this view" }).click();
  await expect(page.locator(".ibpl-more-pop")).toContainText(first);
});

test("a stepper tap turns auto min-poss off; a filter change turns it back on", async ({
  page,
}) => {
  await stub(page);
  const { answer } = loadGolden("il_summary_default.json");
  const cols = possCols("summary");
  const autoFor = (rows: typeof answer.rows) =>
    Math.min(300, autoMinOn(rows, cols.on) ?? 300);
  const input = page.locator("#min_on_poss");
  await expect(input).toHaveValue(String(autoFor(answer.rows)));
  const before = Number(await input.inputValue());
  await page.getByRole("button", { name: "Increase Min ON Poss" }).click();
  await expect(input).toHaveValue(String(before + 10));
  await expect(page).toHaveURL(new RegExp(`min_on=${before + 10}`));
  // A filter change re-arms auto. Auto only lowers a hand-set minimum
  // (helpers.R setup_onoff_auto_min), so hand-set the maximum first: the
  // re-armed auto value must then bring it down to the team's own value.
  await input.fill("3000");
  await input.press("Enter");
  await expect(page).toHaveURL(/min_on=3000/);
  const team = answer.rows[0];
  const teamRows = answer.rows.filter(
    (r) => Number(r.team_id) === Number(team.team_id),
  );
  const expected = autoMinOn(teamRows, cols.on)!;
  expect(expected).toBeLessThan(3000);
  await filtersButton(page).click();
  await pickTeam(page, String(team.Team));
  await expect(input).toHaveValue(String(expected));
});
