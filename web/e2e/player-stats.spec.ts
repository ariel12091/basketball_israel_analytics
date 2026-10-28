import { test, expect, type Page } from "@playwright/test";
import { loadPlayer, playerPresets } from "../test/playerGoldens";
import { formatNumber } from "../src/components/shiny/DataTable";
import { playerDisplay } from "../src/rules/playerStatsTable";
/** Clears every filter from the filters panel (Filters button, toolbar end). */
async function clearAll(page: Page) {
  const button = page.locator(".chips-drawer-toggle");
  if ((await button.getAttribute("aria-expanded")) !== "true")
    await button.click();
  const panel = page.getByRole("dialog", { name: "Filters" });
  await panel
    .getByRole("button", { name: "Clear all filters", exact: true })
    .click();
  await page.keyboard.press("Escape");
  await expect(panel).toBeHidden();
}
for (const file of playerPresets)
  test(`Player Stats ${file}`, async ({ page }, info) => {
    const { g, answer, rows } = loadPlayer(file),
      errors: string[] = [];
    page.on("pageerror", (e) => errors.push(e.message));
    await page.route("**/api/v1/**", (route) =>
      route.fulfill({
        json: new URL(route.request().url()).pathname.endsWith("/version")
          ? { leagues: [{ league: g.league, version: answer.meta.version }] }
          : answer,
      }),
    );
    await page.goto(
      `/${g.league}/player-stats?${new URLSearchParams(g.query)}`,
    );
    await expect(page.locator(".dataTables_info")).toHaveText(
      `Showing ${rows.length ? 1 : 0} to ${Math.min(50, rows.length)} of ${rows.length} entries`,
    );
    const trs = await page
      .locator(".ibpl-dt-scroll tbody tr")
      .evaluateAll((trs) =>
        trs.map((tr) =>
          Object.fromEntries(
            [...tr.querySelectorAll("td")].map((td) => {
              const cell = td.cloneNode(true) as HTMLElement;
              cell.querySelector(".ibpl-row-name")?.remove();
              return [td.getAttribute("data-column")!, cell.textContent ?? ""];
            }),
          ),
        ),
      );
    for (let i = 0; i < trs.length; i++)
      for (const [id, value] of Object.entries(trs[i]))
        expect(value, `${i} ${id}`).toBe(
          id === "Player" || id === "team_name"
            ? String(rows[i][id] ?? "")
            : formatNumber(rows[i][id], id === "gp" ? 0 : 1),
        );
    if (g.league !== "il")
      await expect(page.locator('th[data-column="dfl"]')).toHaveCount(0);
    expect(
      await page.evaluate(
        () => document.documentElement.scrollWidth <= innerWidth + 1,
      ),
    ).toBe(true);
    if (g.id === "il_per_game" || g.id === "euroleague_per30")
      await page.screenshot({
        path: `test-results/player-${g.id}-${info.project.name}.png`,
        fullPage: true,
      });
    expect(errors).toEqual([]);
  });

for (const league of ["il", "euroleague", "eurocup"] as const)
  test(`Player Stats selection and stat chips ${league}`, async ({ page }) => {
    const { answer } = loadPlayer(`${league}_per_game.json`);
    await page.route("**/api/v1/**", (route) =>
      route.fulfill({
        json: new URL(route.request().url()).pathname.endsWith("/version")
          ? { leagues: [{ league, version: answer.meta.version }] }
          : answer,
      }),
    );
    await page.goto(
      `/${league}/player-stats?season=${league === "il" ? 2026 : 2025}`,
    );
    await expect(page.locator(".dataTables_info")).toContainText("entries");
    const player = answer.players.find((p) =>
      answer.rows.some(
        (r) => r.team_id === p.team_id && r.player_id === p.player_id,
      ),
    )!;
    const key = `${player.team_id}:${player.player_id}`;
    const canonical =
      answer.identities.find((r) => `${r.team_id}:${r.player_id}` === key)
        ?.display_name ?? String(player.player_name);
    await page.locator("#ts_players-selectized").fill(canonical);
    // Keep the actual selected team-player key: a canonical name may have two teams.
    await page.getByRole("option").first().click();
    await expect
      .poll(() => new URL(page.url()).searchParams.get("players"))
      .not.toBeNull();
    const selected = new URL(page.url()).searchParams.get("players")!;
    const narrowed = playerDisplay(answer.rows, answer.identities, {
      mode: "Per Game",
      players: [selected],
    }).rows;
    await expect(page.locator(".dataTables_info")).toHaveText(
      `Showing ${narrowed.length ? 1 : 0} to ${Math.min(50, narrowed.length)} of ${narrowed.length} entries`,
    );
    await clearAll(page);
    await page.locator("#ts_stat_filter_add_btn").click();
    await page.locator("#ts_stat_filter_col-selectized").fill("PTS");
    await page
      .getByRole("dialog", { name: "Add stat filter" })
      .getByRole("option", { name: "PTS", exact: true })
      .click();
    await page.locator("#ts_stat_filter_value").fill("10");
    await page
      .getByRole("dialog", { name: "Add stat filter" })
      .getByRole("button", { name: "Add", exact: true })
      .click();
    await expect(
      page.getByRole("link", { name: "Remove PTS filter" }),
    ).toBeVisible();
    const filtered = playerDisplay(answer.rows, answer.identities, {
      mode: "Per Game",
      filters: [{ col: "pts", op: "ge", value: 10 }],
    }).rows;
    await expect(page.locator(".dataTables_info")).toHaveText(
      `Showing 1 to ${Math.min(50, filtered.length)} of ${filtered.length} entries`,
    );
    await page.getByRole("link", { name: "Remove PTS filter" }).click();
    await expect(page.locator(".dataTables_info")).toContainText(
      `of ${playerDisplay(answer.rows, answer.identities, { mode: "Per Game" }).rows.length} entries`,
    );
  });
for (const league of ["il", "euroleague", "eurocup"] as const)
  test(`Player Stats interactions ${league}`, async ({ page }, info) => {
    const { answer } = loadPlayer(`${league}_per_game.json`);
    const requests: string[] = [];
    await page.route("**/api/v1/**", (route) => {
      const url = new URL(route.request().url());
      if (url.pathname.endsWith("/version"))
        return route.fulfill({
          json: { leagues: [{ league, version: answer.meta.version }] },
        });
      requests.push(url.search);
      return route.fulfill({ json: answer });
    });
    await page.goto(
      `/${league}/player-stats?season=${league === "il" ? 2026 : 2025}`,
    );
    await expect(page.locator(".dataTables_info")).toContainText("entries");
    // Mode and display narrowing are local: no repeat SQL request.
    const count = requests.length;
    if (info.project.name === "phone")
      await page
        .getByRole("button", { name: "Toggle navigation", exact: true })
        .click();
    else
      await page.locator('.nav-link[data-value="traditional_stats"]').hover();
    await page
      .getByRole("menuitemradio", { name: "Per 30 Minutes", exact: true })
      .click();
    await expect(page.locator('th[data-column="base_min_pg"]')).toBeVisible();
    expect(requests.length).toBe(count);
    // Min GP is a stepper: typed values are kept on Enter.
    await page.locator("#ts_min_gp").fill("10");
    await page.locator("#ts_min_gp").press("Enter");
    await expect(page.locator("#ts_min_gp")).toHaveValue("10");
    const ids = () =>
      page
        .locator(".ibpl-dt-scroll thead th")
        .evaluateAll((ths) => ths.map((th) => th.getAttribute("data-column")));
    await page.getByText("Column order", { exact: true }).click();
    await page
      .getByLabel("Reorder column", { exact: true })
      .selectOption("pts");
    const before = await ids(),
      index = before.indexOf("pts");
    await page.getByRole("button", { name: "Move left", exact: true }).click();
    expect((await ids()).indexOf("pts")).toBe(index - 1);
    await page.reload();
    await expect(page.locator(".dataTables_info")).toContainText("entries");
    expect((await ids()).indexOf("pts")).toBe(index - 1);
    const download = page.waitForEvent("download");
    await page
      .getByRole("button", { name: "Download CSV", exact: true })
      .click();
    expect((await download).suggestedFilename()).toContain("player_stats");
    // Clear all shows once a panel filter is set, and resets the whole tab
    // (Min GP and display mode too), as Shiny's Reset Filters did.
    await page.locator(".chips-drawer-toggle").click();
    await page
      .getByRole("dialog", { name: "Filters" })
      .getByRole("checkbox", { name: "Clutch", exact: true })
      .check();
    await clearAll(page);
    await expect(page.locator("#ts_min_gp")).toHaveValue("1");
    await expect(page.locator('th[data-column="base_min_pg"]')).toHaveCount(0);
  });
