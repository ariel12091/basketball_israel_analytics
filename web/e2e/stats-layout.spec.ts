// The shared stats-tab layout (design 2026-10-28), proven on On/Off with the
// stored Shiny answers: filters drawer, steppers, view tabs, key, columns.
import { test, expect, type Page } from "@playwright/test";
import { loadGolden } from "../test/onoffGoldens";
import ui from "../src/data/shinyUi.json";
import layout from "../src/data/layoutUi.json";
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

test("filters open right under the Filters button and close on Escape", async ({
  page,
}, info) => {
  await stub(page);
  await expect(page.locator(".ibpl-filter-col")).toHaveCount(0);
  await expect(drawer(page)).toBeHidden();
  await filtersButton(page).click();
  await expect(drawer(page)).toBeVisible();
  // Every section opens expanded.
  for (const title of ["Dates and teams", "Lineup starters", "Game filters"])
    await expect(
      drawer(page).getByRole("button", { name: title }),
    ).toHaveAttribute("aria-expanded", "true");
  const button = (await filtersButton(page).boundingBox())!;
  const panel = (await drawer(page).boundingBox())!;
  expect(panel.y).toBeGreaterThanOrEqual(button.y + button.height);
  expect(panel.y - (button.y + button.height)).toBeLessThan(20);
  if (info.project.name === "desktop")
    expect(
      Math.abs(panel.x + panel.width - (button.x + button.width)),
    ).toBeLessThan(4);
  else expect(panel.width).toBeGreaterThan(page.viewportSize()!.width - 40);
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

test("the legend is a compact key at the table's top right", async ({
  page,
}) => {
  await stub(page);
  const legend = page.locator(".table-legend");
  const table = page.locator(".ibpl-dt-scroll");
  // Above the table, right-aligned with it.
  await expect
    .poll(async () => {
      const l = (await legend.boundingBox())!;
      const t = (await table.boundingBox())!;
      return (
        l.y + l.height <= t.y && Math.abs(l.x + l.width - (t.x + t.width)) < 4
      );
    })
    .toBe(true);
  for (const word of ["worse", "better", "2P%", "3P%", "shot mix"])
    await expect(legend).toContainText(word);
  await expect(page.locator(".legend-box")).toHaveCount(0);
  // A tag pill explains itself in one sentence.
  await legend.getByRole("button", { name: "3PT luck" }).click();
  await expect(page.locator(".ibpl-more-pop")).toContainText(
    layout.tags.luck.text,
  );
  // With Shiny's worked example.
  await expect(page.locator(".ibpl-more-pop")).toContainText("Example:");
  await page.keyboard.press("Escape");
  await page.getByRole("tab", { name: "Four Factors" }).click();
  await expect(page).toHaveURL(/view=four_factors/);
  await expect(legend).toContainText("worse");
  await expect(legend).not.toContainText("shot mix");
  await expect(legend.getByRole("button", { name: "3PT luck" })).toHaveCount(0);
});

test("the view intro is one visible line; More opens the rest", async ({
  page,
}) => {
  await stub(page);
  const blocks = ui.onoff.il.views.summary as {
    kind: string;
    intro?: { t?: string }[];
    bullets?: { t?: string }[][];
  }[];
  const explainer = blocks.find((b) => b.kind === "explainer")!;
  const intro = page.locator(".view-intro");
  await expect(intro).toContainText(layout.intro.onoff.summary);
  await expect(intro).not.toContainText(
    String(explainer.intro![0].t ?? "").slice(0, 30),
  );
  await expect(intro).not.toContainText("3PT luck");
  await intro.getByRole("button", { name: "More" }).click();
  await expect(intro).toContainText(
    String(explainer.intro![0].t ?? "").slice(0, 30),
  );
  await expect(intro).toContainText(
    String(explainer.bullets![0][0].t ?? "").slice(0, 20),
  );
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

test("possession columns are on screen without scrolling sideways", async ({
  page,
}) => {
  await stub(page);
  for (const view of ["Summary", "Four Factors", "Shot Profile"]) {
    await page.getByRole("tab", { name: view }).click();
    await expect(
      page.locator(".ibpl-dt-scroll tbody tr").first(),
    ).toBeVisible();
    for (const label of ["On-court Poss", "Off-court Poss"])
      await expect
        .poll(
          () =>
            page.evaluate((label) => {
              const scroll = document.querySelector(".ibpl-dt-scroll");
              const th = [
                ...document.querySelectorAll(".ibpl-dt-scroll thead th"),
              ].find((t) => t.textContent?.trim() === label);
              if (!scroll || !th) return Infinity;
              return (
                th.getBoundingClientRect().right -
                scroll.getBoundingClientRect().right
              );
            }, label),
          { message: `${view}: ${label}` },
        )
        .toBeLessThanOrEqual(1);
  }
});

test.describe("on touch, every marker opens with a tap", () => {
  test.use({ hasTouch: true, isMobile: true });
  test("header, tag and shot cell open by tap; header arrows still sort", async ({
    page,
  }, info) => {
    test.skip(info.project.name !== "phone", "touch at phone width");
    await stub(page);
    const pop = page.locator(".ibpl-more-pop");
    const netHead = page
      .locator(".ibpl-dt-scroll thead th")
      .filter({ has: page.locator(".ibpl-more", { hasText: /^Net$/ }) })
      .first();
    await netHead.locator(".ibpl-more").tap();
    await expect(pop).toBeVisible();
    await netHead.locator(".ibpl-more").tap();
    await expect(pop).toHaveCount(0);
    // A tap on the header outside its label sorts.
    const before = await page
      .locator('.ibpl-dt-scroll tbody td[data-column="Player"]')
      .first()
      .textContent();
    await netHead.locator(".th-sort-target").tap();
    await expect
      .poll(() =>
        page
          .locator('.ibpl-dt-scroll tbody td[data-column="Player"]')
          .first()
          .textContent(),
      )
      .not.toBe(before);
    const tag = page.locator(".ibpl-dt-scroll .onoff-luck-tag").first();
    await tag.tap();
    await expect(pop).toContainText(/3PT luck|Small sample/);
    await expect(pop.locator(".onoff-tip-why")).toBeVisible();
    // A second tap on the same tag closes it.
    await tag.tap();
    await expect(pop).toHaveCount(0);
    await page.locator(".shot-cell-more").first().tap();
    await expect(pop).toContainText("2PT accuracy:");
  });
});

test.describe("review fixes", () => {
  test("clicking a header's label sorts (mouse)", async ({ page }, info) => {
    test.skip(info.project.name !== "desktop", "mouse at desktop width");
    await stub(page);
    const first = () =>
      page
        .locator('.ibpl-dt-scroll tbody td[data-column="Player"]')
        .first()
        .textContent();
    const before = await first();
    await page
      .locator(".ibpl-dt-scroll thead th .ibpl-more", {
        hasText: /^On-court Poss$/,
      })
      .click();
    await expect.poll(first).not.toBe(before);
  });

  test("Escape in a stepper with the drawer closed keeps focus there", async ({
    page,
  }) => {
    await stub(page);
    const input = page.locator("#min_on_poss");
    await input.fill("555");
    await input.press("Escape");
    await expect(input).toBeFocused();
    await expect(input).toHaveValue(/^(?!555$)/);
  });

  test("shot cells name their numbers to assistive tech", async ({ page }) => {
    await stub(page);
    const shot = page.locator(".shot-cell-more").first();
    await expect(shot).toHaveAccessibleName(/2PT \d+%.*3PT \d+%/);
  });

  test("a hand-set minimum can be reset with no filter chips", async ({
    page,
  }) => {
    await stub(page);
    const input = page.locator("#min_on_poss");
    await input.fill("900");
    await input.press("Enter");
    await expect(page).toHaveURL(/min_on=900/);
    await filtersButton(page).click();
    await drawer(page)
      .getByRole("button", { name: "Clear all filters" })
      .click();
    await expect(page).not.toHaveURL(/min_on=900/);
  });
});

test("the panel's Close and Clear all stay pinned while it scrolls", async ({
  page,
}) => {
  const { g } = await stub(page);
  await filtersButton(page).click();
  await pickTeam(page, String(g.expected.data[0].Team));
  const panel = drawer(page);
  const close = panel.getByRole("button", { name: "Close filters" });
  const clear = panel.getByRole("button", { name: "Clear all filters" });
  const top = (await panel.boundingBox())!.y;
  await panel.evaluate((el) => (el.scrollTop = el.scrollHeight));
  await expect
    .poll(async () => (await close.boundingBox())!.y - top)
    .toBeLessThan(40);
  await expect(clear).toBeInViewport();
  await expect
    .poll(async () => (await clear.boundingBox())!.y - top)
    .toBeLessThan(110);
});
