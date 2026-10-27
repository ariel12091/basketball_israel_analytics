import { test, expect, type Page } from "@playwright/test";
import { loadLineup, pageAnswer, presets } from "../test/lineupGoldens";
import { lineupAutoMin } from "../src/rules/lineups";
import { loadGolden } from "../test/onoffGoldens";

for (const league of ["il", "euroleague"] as const)
  test(`Lineups ${league} On/Off player pivot retains season`, async ({
    page,
  }) => {
    const onoff = loadGolden(`${league}_summary_default.json`),
      lineups = loadLineup(`${league}_summary_default.json`);
    await page.route("**/api/v1/**", (route) => {
      const path = new URL(route.request().url()).pathname;
      return route.fulfill({
        json: path.endsWith("/version")
          ? { leagues: [{ league, version: onoff.answer.meta.version }] }
          : path.endsWith("/onoff")
            ? onoff.answer
            : path.endsWith("/lineup-roster")
              ? lineups.roster
              : pageAnswer(lineups.answer, route.request().url()),
      });
    });
    const season = league === "il" ? "2026" : "2025";
    await page.goto(`/${league}/onoff?season=${season}`);
    const player = page
      .locator('.ibpl-dt-scroll tbody td[data-column="Player"]')
      .first();
    await expect(player).toBeVisible();
    await player.scrollIntoViewIfNeeded();
    await player.evaluate(
      () =>
        new Promise<void>((resolve) =>
          requestAnimationFrame(() => requestAnimationFrame(() => resolve())),
        ),
    );
    await player.click();
    const item = page.getByRole("menuitem", {
      name: "Lineups with this player",
    });
    await item.click();
    await expect(page).toHaveURL(
      new RegExp(`/${league}/lineups\\?season=${season}.*players_on=`),
    );
    await expect(page.locator(".nav-link.active")).toContainText("Lineup Data");
    const selectedPlayer = new URL(page.url()).searchParams.get("players_on");
    await expect(
      page.locator(`.lineup-chip[data-id="${selectedPlayer}"]`),
    ).toHaveAttribute("data-state", "on");
  });
async function fixedBar(page: Page, id: string, value: number) {
  const slider = page.locator(`#${id}`),
    track = page.locator(".irs--shiny").filter({ has: slider });
  const box = (await track.boundingBox())!,
    max = Number(await slider.getAttribute("aria-valuemax"));
  const x = box.x + 11 + (value / max) * (box.width - 22);
  await page.mouse.move(x, box.y + 12);
  await page.mouse.down();
  await page.mouse.up();
  await expect(slider).toHaveAttribute("aria-valuenow", String(value));
}
for (const file of presets)
  test(`Lineups ${file}`, async ({ page }, info) => {
    const { g, answer, roster, games, local, rows } = loadLineup(file);
    await page.route("**/api/v1/**", (route) => {
      const path = new URL(route.request().url()).pathname;
      return route.fulfill({
        json: path.endsWith("/version")
          ? { leagues: [{ league: g.league, version: answer.meta.version }] }
          : path.endsWith("/lineup-roster")
            ? roster
            : path.endsWith("/lineup-games")
              ? games
              : pageAnswer(answer, route.request().url()),
      });
    });
    const errors: string[] = [];
    page.on("pageerror", (e) => errors.push(e.message));
    await page.goto(`/${g.league}/lineups?${new URLSearchParams(g.query)}`);
    const id = g.league === "il" ? "ld_minposs" : "euro_ld_minposs";
    await expect(page.locator(`#${id}`)).toHaveAttribute(
      "aria-valuenow",
      String(lineupAutoMin(local) ?? 20),
    );
    await fixedBar(page, id, 20);
    const table = page.locator(".ibpl-dt-scroll table");
    if (g.league !== "il")
      await expect(
        page
          .locator(".filter-chip")
          .filter({ hasText: `${g.query.size ?? 5}-player units` }),
      ).toContainText(
        `${local.filter((r) => Number(r.total_poss) >= 20).length} shown`,
      );
    await expect(page.locator(".dataTables_info")).toHaveText(
      `Showing ${rows.length ? 1 : 0} to ${Math.min(50, rows.length)} of ${rows.length.toLocaleString("en-US")} entries`,
    );
    const visible = await table
      .locator("tbody tr")
      .evaluateAll((trs) =>
        trs.map((tr) =>
          Object.fromEntries(
            [...tr.querySelectorAll("td[data-column]")].map((td) => [
              td.getAttribute("data-column")!,
              td.textContent?.trim(),
            ]),
          ),
        ),
      );
    for (const [i, r] of visible.entries())
      for (const [k, v] of Object.entries(r)) {
        if (["Off Shot", "Def Shot"].includes(k)) continue;
        const want = rows[i]?.[k];
        if (typeof want === "number")
          expect(
            Math.abs(parseFloat(v?.replaceAll(",", "") ?? "") - want),
            `${g.id} row ${i} ${k}`,
          ).toBeLessThanOrEqual(k === "num_starters" ? 0.501 : 0.051);
        else if (want != null) expect(v).toContain(String(want));
      }
    if (rows.length) {
      await table.locator('th[data-column="off_ppp"]').click();
      await expect(table.locator("tbody tr").first()).toContainText("TOTAL");
    }
    expect(
      await page.evaluate(
        () => document.documentElement.scrollWidth <= innerWidth,
      ),
    ).toBe(true);
    expect(errors).toEqual([]);
    if (g.id.endsWith("_default"))
      await page.screenshot({
        path: `../output/playwright/lineups-${g.league}-${g.view}-${info.project.name}.png`,
        fullPage: true,
      });
  });

for (const league of ["il", "euroleague"] as const)
  test(`Lineups ${league} interactions`, async ({ page }, info) => {
    const loaded = loadLineup(`${league}_summary_default.json`);
    await page.route("**/api/v1/**", (route) => {
      const url = new URL(route.request().url()),
        view = url.searchParams.get("view") ?? "summary";
      const { answer, roster, games } =
        view === "four_factors"
          ? loadLineup(`${league}_four_factors_default.json`)
          : loaded;
      return route.fulfill({
        json: url.pathname.endsWith("/version")
          ? { leagues: [{ league, version: answer.meta.version }] }
          : url.pathname.endsWith("/lineup-roster")
            ? roster
            : url.pathname.endsWith("/lineup-games")
              ? games
              : pageAnswer(answer, url.toString()),
      });
    });
    await page.goto(
      `/${league}/lineups?season=${league === "il" ? 2026 : 2025}`,
    );
    const prefix = league === "il" ? "ld" : "euro_ld";
    await expect(
      page.locator(".ibpl-dt-scroll tbody tr").first(),
    ).toContainText("TOTAL");
    if (info.project.name === "phone")
      await page
        .getByRole("button", { name: "Show Filters", exact: true })
        .click();
    const picker = page.getByRole("combobox", { name: "Team", exact: true });
    const teamName = loaded.answer.rows[0].Team as string;
    await picker.fill(teamName.slice(0, 6));
    await picker.press("ArrowDown");
    await picker.press("Enter");
    await picker.press("Escape");
    await expect(page).toHaveURL(/team=/);
    const roster = page.locator(".lineup-chips-list");
    await expect(roster.locator("button.lineup-chip").first()).toBeVisible();
    // The folded roster keeps selected players visible; use visible rotation chips.
    const visible = roster.locator("button[data-id]");
    const first = visible.nth(0),
      id = await first.getAttribute("data-id");
    await first.click();
    await expect(page).toHaveURL(new RegExp(`players_on=${id}`));
    await page.getByRole("radio", { name: "Any of", exact: true }).click();
    await visible.nth(1).click();
    await visible.nth(2).click();
    await visible.nth(3).click();
    await page
      .getByRole("button", { name: "Switch between at least and exactly" })
      .click();
    await page.getByRole("button", { name: "Change any-of count" }).click();
    await expect(page).toHaveURL(/any_exact=true/);
    await expect(page).toHaveURL(/any_count=2/);
    await page.getByRole("radio", { name: "Off", exact: true }).click();
    await first.click();
    await expect(first).toHaveAttribute("data-state", "off");
    await page.screenshot({
      path: `../output/playwright/lineups-roster-${league}-${info.project.name}.png`,
      fullPage: true,
    });
    await page.reload();
    if (info.project.name === "phone")
      await page
        .getByRole("button", { name: "Show Filters", exact: true })
        .click();
    await expect(page.locator(`.lineup-chip[data-id="${id}"]`)).toHaveAttribute(
      "data-state",
      "off",
    );
    await page.getByRole("button", { name: "Clear", exact: true }).click();
    await expect(page).not.toHaveURL(/players_on=|players_any=|players_off=/);
    const slider = page.locator(`#${prefix}_minposs`),
      target = Number(await slider.getAttribute("aria-valuenow"));
    await slider.press("ArrowRight");
    await expect(slider).toHaveAttribute("aria-valuenow", String(target + 10));
    await page.getByRole("radio", { name: "2", exact: true }).click();
    await expect(page).toHaveURL(/size=2/);
    await page.getByRole("checkbox", { name: "Clutch", exact: true }).check();
    await expect(
      page.getByRole("slider", { name: "Max point margin" }),
    ).toBeVisible();
    await page
      .getByRole("button", { name: "Reset Lineup Filters", exact: true })
      .click();
    await expect(page).not.toHaveURL(/size=2|clutch=true|team=/);
    await expect(page.locator("button.ibpl-lineup-link").first()).toBeVisible();
    await page.locator("button.ibpl-lineup-link").first().click();
    await expect(page.getByRole("dialog")).toBeVisible();
    await expect(
      page.getByRole("dialog").locator("tbody tr").first(),
    ).toBeVisible();
    await page.screenshot({
      path: `../output/playwright/lineups-modal-${league}-${info.project.name}.png`,
      fullPage: true,
    });
    await page.keyboard.press("Escape");
    await expect(page.getByRole("dialog")).toHaveCount(0);
    expect(
      await page.evaluate(
        () => document.documentElement.scrollWidth <= innerWidth,
      ),
    ).toBe(true);
  });
