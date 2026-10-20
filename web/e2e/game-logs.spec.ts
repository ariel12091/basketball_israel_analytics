import { test, expect } from "@playwright/test";
import { readFileSync } from "node:fs";
import { gameLogs, type Row, type GameLogParams } from "../src/rules/gameLogs";

const presets = [
  "il_summary_team",
  "il_ff_starters",
  "il_summary_last5_home",
  "il_summary_opps_statfilter",
  "il_summary_empty",
  "el_ff_teams_phase",
  "el_summary_round_starters",
];
function urlParams(p: GameLogParams) {
  const q = new URLSearchParams({ season: String(p.season), view: p.view });
  for (const key of [
    "team",
    "teams",
    "game_type",
    "phase",
    "opponents",
    "home_away",
    "outcome",
  ] as const) {
    const value = p[key];
    if (value && String(value)) q.set(key, String(value));
  }
  if (p.dates?.[0]) q.set("start", p.dates[0]);
  if (p.dates?.[1]) q.set("end", p.dates[1]);
  for (const [key, value] of Object.entries(p.gn ?? {}))
    if (value != null) q.set(key, String(value));
  for (const [side, name] of [
    ["off", "own_starters"],
    ["def", "opp_starters"],
  ])
    for (const [bound, op] of [
      ["min", "gte"],
      ["max", "lte"],
    ]) {
      const value =
        p.starters?.[
          `${side}_${bound}` as keyof NonNullable<GameLogParams["starters"]>
        ];
      if (value != null) q.set(name, `${op}:${value}`);
    }
  if (p.stat_filters?.length) q.set("stats", JSON.stringify(p.stat_filters));
  return q;
}
const decimal = new Set([
  "minutes",
  "off_ppp",
  "def_ppp",
  "net_rtg",
  "off_efg_pct",
  "off_oreb_pct",
  "off_tov_pct",
  "off_ftr_pct",
  "def_efg_pct",
  "def_oreb_pct",
  "def_tov_pct",
  "def_ftr_pct",
]);
for (const preset of presets) {
  test(`${preset}: stored Shiny cells, URL restoration, CSV and layout`, async ({
    page,
  }, info) => {
    const g = JSON.parse(
      readFileSync(`test/goldens/game_logs/${preset}.json`, "utf8"),
    );
    const result = gameLogs(g.input, g.input.scoreless ?? [], g.params);
    const expected: Row[] = g.expected?.data ?? [];
    await page.route("**/api/v1/**", async (route) => {
      if (route.request().url().includes("/version"))
        return route.fulfill({ json: { leagues: [] } });
      await route.fulfill({
        json: {
          ...result,
          meta: {
            ...result.meta,
            version: "golden",
            teams: [],
            rounds: [],
            phases: [],
          },
        },
      });
    });
    const errors: string[] = [];
    page.on("pageerror", (e) => {
      errors.push(e.message);
      console.error("Browser error:", e.message);
    });
    page.on("requestfailed", (r) =>
      console.error("Failed request:", r.url(), r.failure()?.errorText),
    );
    await page.goto(`/${g.params.league}/game-logs?${urlParams(g.params)}`);
    await expect(page.getByRole("status")).toHaveText(
      `${expected.length.toLocaleString()} team-games`,
    );
    const cells = await page.locator("tbody tr").evaluateAll((rows) =>
      rows.map((row) =>
        Array.from(row.querySelectorAll("td")).map((td) => ({
          column: td.getAttribute("data-column")!,
          text: td.textContent,
        })),
      ),
    );
    expect(cells).toHaveLength(expected.length);
    for (const [i, row] of cells.entries())
      for (const cell of row) {
        if (["Off Shot", "Def Shot"].includes(cell.column)) {
          expect(cell.text).toContain("2PT");
          expect(cell.text).toContain("3PT");
          continue;
        }
        const value = expected[i][cell.column];
        expect(cell.text, `row ${i} ${cell.column}`).toBe(
          value == null
            ? "—"
            : typeof value === "number"
              ? value.toLocaleString("en-US", {
                  minimumFractionDigits: decimal.has(cell.column) ? 1 : 0,
                  maximumFractionDigits: decimal.has(cell.column) ? 1 : 0,
                })
              : String(value),
        );
      }
    expect(
      await page.evaluate(
        () => document.documentElement.scrollWidth <= innerWidth,
      ),
    ).toBe(true);
    if (info.project.name === "phone") {
      await page
        .getByRole("button", { name: "Show Filters", exact: true })
        .click();
      await expect(
        page.getByRole("button", { name: "Reset", exact: true }),
      ).toBeVisible();
      await page
        .getByRole("button", { name: "Hide Filters", exact: true })
        .click();
    }
    if (expected.length) {
      const download = page.waitForEvent("download");
      await page.getByRole("button", { name: /Download CSV/ }).click();
      const stream = await (await download).createReadStream();
      let csv = "";
      for await (const chunk of stream!) csv += chunk.toString();
      expect(csv.split("\r\n")).toHaveLength(expected.length + 2);
      expect(csv).toContain(String(expected[0].team_name));
    } else
      await expect(
        page.getByText("No games match these filters."),
      ).toBeVisible();
    if (expected.length) {
      const metric =
        g.params.view === "four_factors" ? "def_efg_pct" : "def_ppp";
      const label = metric === "def_ppp" ? "Def PPP" : "Def eFG%";
      const values = expected
        .map((row) => row[metric])
        .filter((v): v is number => typeof v === "number");
      if (values.length) {
        await page
          .getByRole("button", {
            name: new RegExp("^" + label.replace("%", "%") + " "),
          })
          .click();
        await expect(
          page.locator(`td[data-column="${metric}"]`).first(),
        ).toHaveText(Math.min(...values).toFixed(1));
      }
    }
    await page.reload();
    await expect(page.getByRole("status")).toHaveText(
      `${expected.length.toLocaleString()} team-games`,
    );
    expect(errors).toEqual([]);
    if (preset === "il_summary_team")
      await page.screenshot({
        path: `../output/playwright/game-logs-${info.project.name}.png`,
        fullPage: true,
      });
  });
}

test("version refresh and retry recover without a navigation", async ({
  page,
}) => {
  let calls = 0;
  await page.route("**/api/v1/**", async (route) => {
    if (route.request().url().includes("/version"))
      return route.fulfill({
        json: { leagues: [{ league: "il", version: "new" }] },
      });
    calls++;
    if (calls === 1)
      return route.fulfill({
        status: 503,
        json: { error: "Service busy; retry shortly" },
      });
    return route.fulfill({
      json: {
        rows: [],
        meta: {
          version: calls === 2 ? "old" : "new",
          teams: [],
          rounds: [],
          phases: [],
          shot_averages: null,
        },
      },
    });
  });
  await page.goto("/il/game-logs?season=2026");
  await expect(
    page.getByRole("alert").filter({ hasText: "Service busy" }),
  ).toContainText("Service busy");
  await page.getByRole("button", { name: "Retry", exact: true }).click();
  await expect(page.getByText("No games match these filters.")).toBeVisible();
  await page.evaluate(() => window.dispatchEvent(new Event("focus")));
  await expect.poll(() => calls).toBe(3);
});
