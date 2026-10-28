// Real stored Shiny frames, both viewport projects. The API responses were
// captured separately from the local read-only route; the expected rows are R.
import { test, expect, type Page } from "@playwright/test";
import { readFileSync } from "node:fs";
import { loadGolden, presets } from "../test/onoffGoldens";
import { autoMinAll, autoMinOn, possCols } from "../src/rules/onoff";

/** Sets a possession stepper to `target` by typing it, as a user would. */
async function fixedStep(page: Page, id: string, target: number) {
  const input = page.locator(`#${id}`);
  await input.fill(String(target));
  await input.press("Enter");
  await expect(input).toHaveValue(String(target));
  const param = id.includes("min_on") ? "min_on" : "min_all";
  await expect
    .poll(() =>
      Number(
        new URL(page.url()).searchParams.get(param) ??
          (param === "min_on" ? 300 : 100),
      ),
    )
    .toBe(target);
}

for (const file of presets)
  test(`On/Off ${file}`, async ({ page }, info) => {
    if (process.env.ONOFF_DEBUG) {
      page.on("console", (message) => console.log(message.text()));
      await page.addInitScript(() =>
        document.addEventListener(
          "keydown",
          (event) => {
            const el = event.target as HTMLElement;
            console.log(
              "KEY",
              event.key,
              el.id,
              el.getAttribute("aria-valuenow"),
            );
          },
          true,
        ),
      );
    }
    const { g, answer, ps } = loadGolden(file);
    await page.route("**/api/v1/**", (route) => {
      const url = route.request().url();
      return route.fulfill({
        json: url.includes("/version")
          ? { leagues: [{ league: g.league, version: answer.meta.version }] }
          : url.includes("/player-stat-filters")
            ? { rows: ps, meta: { version: answer.meta.version } }
            : answer,
      });
    });
    const errors: string[] = [];
    page.on("pageerror", (e) => errors.push(e.message));
    await page.goto(`/${g.league}/onoff?${new URLSearchParams(g.query)}`);
    await expect(page.locator(".ibpl-dt-scroll table")).toBeVisible();
    const prefix = g.league === "il" ? "" : "euro_";
    // Await the initial auto adjustment before manually restoring the fixed R
    // bars; a visible table can precede nuqs committing the new slider value.
    const teamIds = (g.query.teams ?? "")
      .split(",")
      .filter(Boolean)
      .map(Number);
    const base = answer.rows.filter(
      (r) => !teamIds.length || teamIds.includes(Number(r.team_id)),
    );
    const cols = possCols(g.view);
    const initialOn = Math.min(300, autoMinOn(base, cols.on) ?? 300);
    const allBase =
      g.view === "four_factors"
        ? base.filter((r) => Number(r.off_on_poss) >= initialOn)
        : base;
    const initialAll = Math.min(
      100,
      autoMinAll(allBase, cols.on, cols.on, cols.off) ?? 100,
    );
    await expect(page.locator(`#${prefix}min_on_poss`)).toHaveValue(
      String(initialOn),
    );
    await expect(page.locator(`#${prefix}min_all_poss`)).toHaveValue(
      String(initialAll),
    );
    // testServer fixes these minimums; the browser initially applies auto-min.
    await fixedStep(page, `${prefix}min_on_poss`, 300);
    await fixedStep(page, `${prefix}min_all_poss`, 100);
    const expected = [...g.expected.data].sort((a, b) => {
      const metric =
        g.view === "summary"
          ? "Net RTG Diff"
          : g.view === "four_factors"
            ? "Net Diff"
            : "Off Lay+Dunk Diff";
      return Number(b[metric] ?? -Infinity) - Number(a[metric] ?? -Infinity);
    });
    const length = g.view === "four_factors" ? 50 : 30;
    if (g.view !== "four_factors")
      await expect(page.locator(".dataTables_info")).toHaveText(
        `Showing ${expected.length ? 1 : 0} to ${Math.min(length, expected.length)} of ${expected.length.toLocaleString("en-US")} entries`,
      );
    else
      await expect(
        page.locator('.ibpl-dt-scroll tbody td[data-column="Player"]'),
      ).toHaveCount(Math.min(length, expected.length));
    const visible = await page
      .locator(".ibpl-dt-scroll tbody tr")
      .evaluateAll((trs) =>
        trs.map((tr) =>
          Object.fromEntries(
            Array.from(tr.querySelectorAll("td[data-column]")).map((td) => [
              td.getAttribute("data-column")!,
              {
                text: td.textContent?.trim() ?? "",
                headline: td.querySelector(".diff-val")?.textContent?.trim(),
              },
            ]),
          ),
        ),
      );
    visible.forEach((row, i) => {
      const want = expected[i];
      for (const [col, cell] of Object.entries(row)) {
        const value = want[col];
        if (col === "Team" || col === "Player")
          expect(cell.text).toContain(String(value));
        else if (col.includes("Shot") && g.view === "summary") {
          const side = col.startsWith("Off") ? "off" : "def";
          const phase = col.endsWith("ON") ? "on" : "off";
          const n = (suffix: string) =>
            Number(want[`${side}_${phase}_${suffix}`] ?? 0);
          const a2 = n("fg2_att"),
            a3 = n("fg3_att"),
            total = a2 + a3;
          const text = total
            ? `${a2 ? Math.round((n("fg2_made") / a2) * 100) : 0}%|${a3 ? Math.round((n("fg3_made") / a3) * 100) : 0}%${Math.round((a2 / total) * 100)}%${100 - Math.round((a2 / total) * 100)}%`
            : "-";
          expect(cell.text.replace(/\s/g, "")).toBe(text);
        } else if (typeof value === "number" && !col.includes("Shot")) {
          const primary = cell.headline ?? cell.text;
          // Range cells have on/off and an impact line below the headline.
          const parsed = parseFloat(primary.replaceAll(",", ""));
          const tolerance = col.includes("Poss")
            ? 0
            : g.view === "summary" &&
                ![
                  "minutes",
                  "Off ON PPP",
                  "Def ON PPP",
                  "Off OFF PPP",
                  "Def OFF PPP",
                ].includes(col)
              ? 0.0051
              : 0.051;
          expect(
            Math.abs(parsed - value),
            `${g.id} row ${i} ${col}`,
          ).toBeLessThanOrEqual(tolerance);
        }
      }
    });
    expect(errors).toEqual([]);
    expect(
      await page.evaluate(
        () => document.documentElement.scrollWidth <= innerWidth,
      ),
    ).toBe(true);
    if (g.id.endsWith("_default")) {
      await page.screenshot({
        path: `../output/playwright/onoff-${g.league}-${g.view}-${info.project.name}.png`,
        fullPage: true,
      });
      if (g.view === "four_factors") {
        await expect(
          page.locator("th.group-head").filter({ hasText: "Offense Impact" }),
        ).toHaveAttribute("colspan", "5");
        await expect(
          page.locator("th.group-head").filter({ hasText: "Defense Impact" }),
        ).toHaveAttribute("colspan", "5");
      }
      if (g.view === "summary") {
        const net = page.locator('.onoff-net-tip[data-tip-kind="all"]').first();
        await net.click();
        await expect(page.locator(".ibpl-more-pop")).toContainText("3PT luck");
        await page.mouse.move(0, 0);
        if (g.league === "il") {
          const download = page.waitForEvent("download");
          await page
            .getByRole("button", { name: "Download CSV", exact: true })
            .click();
          const file = await download;
          expect(file.suggestedFilename()).toMatch(/^onoff_summary_.*\.csv$/);
          const csv = readFileSync((await file.path())!, "utf8");
          expect(csv.split("\r\n")).toHaveLength(expected.length + 1);
          expect(csv).toMatch(
            /^"Team","Player","Min","On Poss","Off Poss","Net",/,
          );
        }
      }
    }
  });

for (const league of ["il", "euroleague", "eurocup"] as const)
  test(`On/Off ${league} interactions`, async ({ page }, info) => {
    const pick = (view: string) => loadGolden(`${league}_${view}_default.json`);
    await page.route("**/api/v1/**", (route) => {
      const url = new URL(route.request().url());
      const view = url.searchParams.get("view") ?? "summary";
      const { answer } = pick(view);
      return route.fulfill({
        json: url.pathname.endsWith("/version")
          ? { leagues: [{ league, version: answer.meta.version }] }
          : answer,
      });
    });
    const { g } = pick("summary");
    await page.goto(`/${league}/onoff?${new URLSearchParams(g.query)}`);
    const table = page.locator(".ibpl-dt-scroll");
    await expect(table.locator("tbody tr").first()).toBeVisible();
    await page.locator(".chips-drawer-toggle").click();
    const teams = page.getByRole("combobox", {
      name: "Teams",
      exact: true,
    });
    const teamName = String(g.expected.data[0].Team);
    await teams.fill(teamName);
    await teams.press("ArrowDown");
    await teams.press("Enter");
    await teams.press("Escape");
    await expect(page).toHaveURL(/teams=/);
    await expect(table.locator('td[data-column="Team"]').first()).toContainText(
      teamName,
    );
    const resetAnswer = page.waitForResponse((response) => {
      const url = new URL(response.url());
      return url.pathname.endsWith("/onoff") && !url.searchParams.has("teams");
    });
    await page
      .getByRole("dialog", { name: "Filters" })
      .getByRole("button", { name: "Clear all filters", exact: true })
      .click();
    await (await resetAnswer).finished();
    await page.evaluate(
      () =>
        new Promise<void>((resolve) =>
          requestAnimationFrame(() => requestAnimationFrame(() => resolve())),
        ),
    );
    await expect(
      page.locator(league === "il" ? "#onoff_dt" : "#euro_dt"),
    ).toHaveAttribute("aria-busy", "false");
    await expect(page).not.toHaveURL(/teams=/);
    const prefix = league === "il" ? "" : "euro_";
    await page.keyboard.press("Escape");
    await expect(page.getByRole("dialog", { name: "Filters" })).toBeHidden();
    const stepper = page.locator(`#${prefix}min_on_poss`);
    const before = Number(await stepper.inputValue());
    await page.getByRole("button", { name: "Decrease Min ON Poss" }).click();
    await expect(stepper).toHaveValue(String(before - 10));
    // A typed value off the step rounds to it.
    await stepper.fill(String(before - 17));
    await stepper.press("Enter");
    await expect(stepper).toHaveValue(
      String(Math.round((before - 17) / 10) * 10),
    );
    const player = table.locator('td[data-column="Player"]').first();
    await expect(async () => {
      await player.scrollIntoViewIfNeeded();
    }).toPass({ timeout: 5000 });
    await player.evaluate(
      () =>
        new Promise<void>((resolve) =>
          requestAnimationFrame(() => requestAnimationFrame(() => resolve())),
        ),
    );
    await player.click();
    await expect(
      page.getByRole("menuitem", { name: "Lineups with this player" }),
    ).toBeVisible();
    await page.keyboard.press("Escape");
    await expect(page.getByRole("menu")).toHaveCount(0);
    if (info.project.name === "phone")
      await page.locator(".navbar-toggle").click();
    const activeTab = page.locator(".nav-item", {
      has: page.locator(".nav-link.active"),
    });
    if (info.project.name !== "phone") await activeTab.hover();
    await activeTab.locator('.thm-item[data-value="Four Factors"]').click();
    await expect(
      table.locator("td.onoff-factor-cell .sub-text").first(),
    ).toBeVisible();
    await page
      .getByRole("button", { name: "Hide on/off detail", exact: true })
      .click();
    await expect(table.locator("td.onoff-factor-cell .sub-text")).toHaveCount(
      0,
    );
    await expect(
      table.locator("td.onoff-factor-cell .diff-val").first(),
    ).toBeVisible();
    await page
      .getByRole("button", { name: "Show on/off detail", exact: true })
      .click();
    await expect(
      table.locator("td.onoff-factor-cell .sub-text").first(),
    ).toBeVisible();
    await expect(page.locator("body")).not.toHaveClass(/ff-ranges-off/);
    expect(
      await page.evaluate(
        () => document.documentElement.scrollWidth <= innerWidth,
      ),
    ).toBe(true);
  });
