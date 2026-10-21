// The Game Logs controls as Shiny has them (ui_tab4_gamelogs.R,
// ui_tab11_euro_gamelogs.R, build_filter_chips(), stat_filter_chips_ui(),
// app.js): searchable selectize pickers (Israeli team single with
// "-- All teams --", EuroLeague teams multi), opponents, named chips with
// clears and "Clear all", the navbar view menu, the stat-filter popover
// (Israeli only), explainer toggles, the glossary and the filter-panel toggle.
import { test, expect } from "@playwright/test";

const teams = [
  { id: 701, name: "Hapoel Tel-Aviv" },
  { id: 702, name: "AS Monaco" },
  { id: 703, name: "Real Madrid" },
];
const row = {
  game_id: 1,
  team_id: 702,
  gn: 1,
  round_number: 1,
  game_type_label: "Regular season",
  phase_label: "Regular Season",
  game_date: "2025-10-01",
  team_name: "AS Monaco",
  opp_team_name: "Real Madrid",
  result: "W",
  score_display: "80-70",
  score: "80-70",
  minutes: 40,
  off_ppp: 110,
  def_ppp: 100,
  net_rtg: 10,
  off_poss: 72,
  def_poss: 70,
};

for (const league of ["il", "euroleague", "eurocup"] as const) {
  test(`[GL-01][GL-02][X-10] ${league}: pickers, chips, view menu, popover, glossary`, async ({
    page,
  }, info) => {
    const euro = league !== "il";
    const requests: string[] = [];
    await page.route("**/api/v1/**", (route) => {
      const url = route.request().url();
      if (url.includes("/version"))
        return route.fulfill({
          json: { leagues: [{ league: "il", version: "2026-10-20 21:41:48" }] },
        });
      requests.push(url);
      return route.fulfill({
        json: {
          rows: [row],
          meta: {
            version: "test",
            teams,
            rounds: [1, 2, 3],
            phases: ["RS"],
            seasons: ["2025", "2024"],
            shot_averages: null,
          },
        },
      });
    });
    // Season 2024 has no catalogue roster, so the Israeli pickers use the API's.
    await page.goto(`/${league}/game-logs?season=${euro ? 2025 : 2024}`);
    await expect(page.locator(".dataTables_info")).toHaveText(
      "Showing 1 to 1 of 1 entries",
    );
    await expect(page.locator(".navbar .nav-link.active")).toContainText(
      "Game Logs",
    );
    await expect(page.locator("#last_updated")).toHaveText("Oct 20");

    const phone = info.project.name === "phone";
    if (phone)
      await page
        .getByRole("button", { name: "Show Filters", exact: true })
        .click();
    const prefix = euro ? "eurogl" : "gl";
    const picker = page.locator(
      `#${prefix}_${euro ? "teams" : "team"}-selectized`,
    );
    await picker.click();
    if (!euro)
      await expect(
        page.getByRole("option", { name: "-- All teams --" }),
      ).toBeVisible();
    await picker.fill("monaco");
    await expect(
      page.getByRole("option", { name: "AS Monaco", exact: true }),
    ).toBeVisible();
    await expect(
      page.getByRole("option", { name: "Real Madrid", exact: true }),
    ).toHaveCount(0);
    await picker.press("Enter");
    const key = euro ? "teams" : "team";
    await expect
      .poll(() => new URL(page.url()).searchParams.get(key))
      .toBe("702");
    const teamChip = page.locator(".filter-chips .filter-chip", {
      hasText: "AS Monaco",
    });
    await expect(teamChip).toBeVisible();
    if (euro) {
      await picker.fill("madrid");
      await picker.press("Enter");
      await expect
        .poll(() => new URL(page.url()).searchParams.get("teams"))
        .toBe("702,703");
      await expect(
        page.locator(".filter-chips .filter-chip", { hasText: "2 teams" }),
      ).toBeVisible();
      await picker.press("Escape");
    }

    const opponents = page.locator(`#${prefix}_opponents-selectized`);
    await opponents.click();
    await opponents.fill("hapoel");
    await page
      .getByRole("option", { name: "Hapoel Tel-Aviv", exact: true })
      .click();
    await expect
      .poll(() => new URL(page.url()).searchParams.get("opponents"))
      .toBe("701");
    await expect(
      page.locator(".filter-chips .filter-chip", {
        hasText: "vs Hapoel Tel-Aviv",
      }),
    ).toBeVisible();
    await opponents.press("Backspace");
    await expect
      .poll(() => new URL(page.url()).searchParams.get("opponents"))
      .toBe(null);
    await opponents.press("Escape");

    // Last N clears a GN/round range (setup_gn_last_n_sync()).
    const gnMin = page.locator(`#${prefix}_gn_min-selectized`);
    await gnMin.click();
    await page.getByRole("option", { name: "2", exact: true }).click();
    await expect(
      page.locator(".filter-chips .filter-chip", {
        hasText: `${euro ? "Rd" : "GN"}>=2`,
      }),
    ).toBeVisible();
    const lastN = page.locator(`#${prefix}_last_n-selectized`);
    await lastN.click();
    await page.getByRole("option", { name: "3", exact: true }).click();
    await expect
      .poll(() => new URL(page.url()).searchParams.get("min_gn"))
      .toBe(null);
    await expect(
      page.locator(".filter-chips .filter-chip", { hasText: "Last 3 games" }),
    ).toBeVisible();

    await page.reload();
    await expect(
      page.locator(".filter-chips .filter-chip", {
        hasText: euro ? "2 teams" : "AS Monaco",
      }),
    ).toBeVisible();
    await expect(
      page.locator(".filter-chips .filter-chip", { hasText: "Last 3 games" }),
    ).toBeVisible();
    await page
      .locator(".filter-chips .filter-chip", { hasText: "Last 3 games" })
      .getByRole("button")
      .click();
    await expect
      .poll(() => new URL(page.url()).searchParams.get("last_n"))
      .toBe(null);

    if (!euro) {
      await page.locator("#gl_stat_filter_add_btn").click();
      await page.locator("#gl_stat_filter_col-selectized").click();
      await page.getByRole("option", { name: "Off PPP", exact: true }).click();
      await page.locator("#gl_stat_filter_value").fill("105");
      await page.getByRole("button", { name: "Add", exact: true }).click();
      await expect(page.locator(".filter-chip.chip-stat")).toHaveText(
        /Off PPP >= 105/,
      );
      await expect
        .poll(() => new URL(page.url()).searchParams.get("stats"))
        .toBe('[{"col":"off_ppp","op":"ge","value":105}]');
    } else await expect(page.locator(".filter-chip-add")).toHaveCount(0);

    await page.locator(".chip-clear-all").click();
    await expect
      .poll(() => new URL(page.url()).searchParams.get(key))
      .toBe(null);
    await expect(page.locator(".filter-chip.chip-stat")).toHaveCount(0);

    // The navbar view menu (app.js CFG) switches to Four Factors.
    if (phone) await page.locator(".navbar-toggle").click();
    const gl = page.locator(".nav-item", {
      has: page.locator(".nav-link.active"),
    });
    if (!phone) await gl.hover();
    await gl.locator('.thm-item[data-value="Four Factors"]').click();
    await expect
      .poll(() => new URL(page.url()).searchParams.get("view"))
      .toBe("four_factors");
    await expect(
      page.locator(".dataTables_scrollHead th.group-head", {
        hasText: "Offense",
      }),
    ).toHaveAttribute("colspan", "5");
    await expect(
      page.locator(".dataTables_scrollHead th.group-head", {
        hasText: "Usage",
      }),
    ).toHaveAttribute("colspan", "2");

    // Explainer and glossary.
    const explainer = page
      .locator(".shiny-panel-conditional:visible .js-explainer-toggle")
      .first();
    await explainer.click();
    await expect(
      page.locator(".shiny-panel-conditional:visible .explainer-body"),
    ).toHaveClass(/show/);
    if (phone && !(await page.locator("#open_glossary").isVisible()))
      await page.locator(".navbar-toggle").click();
    await page.locator("#open_glossary").click();
    await expect(page.getByRole("dialog", { name: "Glossary" })).toBeVisible();
    await page.keyboard.press("Escape");
    await expect(page.getByRole("dialog", { name: "Glossary" })).toHaveCount(0);

    // The chips-row Filters button collapses the filter column (app.js).
    if (!phone) {
      await page.locator(".js-filters-toggle").click();
      await expect(page.locator("body")).toHaveClass(/filters-collapsed/);
      await page.locator(".js-filters-toggle").click();
      await expect(page.locator("body")).not.toHaveClass(/filters-collapsed/);
    }
    expect(
      await page.evaluate(
        () => document.documentElement.scrollWidth <= innerWidth,
      ),
    ).toBe(true);
    expect(requests.every((u) => !u.includes("own_starters=gte%3A&"))).toBe(
      true,
    );
  });
}

test("[X-01] one layout per width, mobile first", async ({ page }, info) => {
  await page.route("**/api/v1/**", (route) =>
    route.fulfill({
      json: {
        leagues: [],
        rows: [],
        meta: {
          version: "t",
          teams: [],
          rounds: [],
          phases: [],
          shot_averages: null,
        },
      },
    }),
  );
  await page.goto("/il/game-logs");
  const body = page.locator("body");
  if (info.project.name === "phone") {
    await expect(body).toHaveClass(/ibpl-mobile/);
    await expect(body).toHaveClass(/ibpl-collapsed-nav/);
  } else {
    await expect(body).toHaveClass(/ibpl-device-desktop/);
    await expect(body).not.toHaveClass(/ibpl-mobile/);
  }
});
