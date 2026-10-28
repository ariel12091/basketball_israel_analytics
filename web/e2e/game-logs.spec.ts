// The Game Logs page against the stored Shiny goldens (app/tests/goldens,
// exported by scripts/export_goldens_json.R): every preset, at the phone and
// desktop widths of playwright.config.ts. The API is intercepted with the
// shared rules' answer for the preset; the page must show Shiny's table:
// DataTables paging (50 rows), default order date desc then GN/round desc,
// DT formatting (one decimal, thousands marks), DataTables' ascending-first
// header ordering, the CSV of csv_export_button(), and URL restoration.
import { test, expect, type Page } from "@playwright/test";
import { readdirSync, readFileSync } from "node:fs";
import { gameLogs, type Row, type GameLogParams } from "../src/rules/gameLogs";

const DIR = "test/goldens/game_logs";
const presets = readdirSync(DIR)
  .filter((f) => f.endsWith(".json"))
  .map((f) => f.replace(/\.json$/, ""));

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

const ROUND1 = new Set([
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
// DT::formatRound(1) / formatCurrency(digits 0): toFixed plus "," marks.
function shiny(value: unknown, column: string): string {
  if (value == null) return "";
  if (typeof value !== "number") return String(value);
  const fixed = value.toFixed(ROUND1.has(column) ? 1 : 0);
  if (!ROUND1.has(column) && !column.endsWith("_poss")) return String(value);
  const [i, d] = fixed.split(".");
  return i.replace(/\B(?=(\d{3})+(?!\d))/g, ",") + (d ? `.${d}` : "");
}
// DataTables' default order for this table, ties in the frame's order.
function defaultOrder(rows: Row[], euro: boolean): Row[] {
  const second = euro ? "round_number" : "gn";
  return rows
    .map((r, i) => ({ r, i }))
    .sort((a, b) => {
      const d = String(b.r.game_date).localeCompare(String(a.r.game_date));
      if (d) return d;
      const g =
        Number(b.r[second] ?? -Infinity) - Number(a.r[second] ?? -Infinity);
      return g || a.i - b.i;
    })
    .map((x) => x.r);
}
async function visibleCells(page: Page) {
  return page.locator(".ibpl-dt-scroll tbody tr").evaluateAll((rows) =>
    rows.map((row) =>
      Object.fromEntries(
        Array.from(row.querySelectorAll("td")).map((td) => [
          td.getAttribute("data-column")!,
          // The phone name label is part of the first cell's DOM only.
          Array.from(td.childNodes)
            .filter(
              (n) =>
                !(
                  n instanceof HTMLElement &&
                  n.classList.contains("ibpl-row-name")
                ),
            )
            .map((n) => n.textContent)
            .join(""),
        ]),
      ),
    ),
  );
}

for (const preset of presets) {
  test(`[GL-01][GL-02][GL-04][GL-05][GL-07][GL-08] ${preset}: Shiny cells, order, paging, CSV, URL`, async ({
    page,
  }) => {
    const g = JSON.parse(readFileSync(`${DIR}/${preset}.json`, "utf8"));
    const euro = g.params.league !== "il";
    const result = gameLogs(g.input, g.input.scoreless ?? [], g.params);
    const expected = defaultOrder(g.expected?.data ?? [], euro);
    await page.route("**/api/v1/**", (route) =>
      route.request().url().includes("/version")
        ? route.fulfill({
            json: {
              leagues: [{ league: "il", version: "2026-10-20 21:41:48" }],
            },
          })
        : route.fulfill({
            json: {
              ...result,
              meta: {
                ...result.meta,
                view: g.params.view,
                version: "golden",
                teams: [],
                rounds: [],
                phases: [],
              },
            },
          }),
    );
    const errors: string[] = [];
    page.on("pageerror", (e) => errors.push(e.message));
    await page.goto(`/${g.params.league}/game-logs?${urlParams(g.params)}`);

    if (!expected.length) {
      // Tab 4 renders nothing; Tab 11 renders empty_game_log().
      if (euro)
        await expect(
          page.getByText("No games match the current filters"),
        ).toBeVisible();
      else {
        await expect(page.locator("#gl_table")).not.toHaveClass(
          /recalculating/,
        );
        await expect(page.locator("#gl_table table")).toHaveCount(0);
      }
      expect(errors).toEqual([]);
      return;
    }
    const total = expected.length;
    const info = page.locator(".dataTables_info");
    await expect(info).toHaveText(
      `Showing 1 to ${Math.min(50, total)} of ${total.toLocaleString("en-US")} entries`,
    );

    const checkPage = async (from: number) => {
      const cells = await visibleCells(page);
      expect(cells).toHaveLength(Math.min(50, total - from));
      cells.forEach((row, i) => {
        const want = expected[from + i];
        for (const [column, text] of Object.entries(row)) {
          if (column === "Off Shot" || column === "Def Shot") {
            expect(text, `row ${from + i} ${column}`).toMatch(
              /^(-|\d+% \| \d+%\d+%\d+%)$/,
            );
            continue;
          }
          if (column === "gameflow") {
            // Shiny suppresses the opener when a game has no scoring feed.
            expect(text).toBe(
              String(want[column] ?? "").replace(/<[^>]*>/g, ""),
            );
            continue;
          }
          expect(text, `row ${from + i} ${column}`).toBe(
            shiny(want[column], column),
          );
        }
      });
    };
    await checkPage(0);
    if (total > 50) {
      await page.locator(".paginate_button.next").click();
      await expect(info).toHaveText(
        `Showing 51 to ${Math.min(100, total)} of ${total.toLocaleString("en-US")} entries`,
      );
      await checkPage(50);
      await page.locator(".paginate_button.previous").click();
    }

    // A first click orders ascending (no orderSequence on this table).
    const metric = g.params.view === "four_factors" ? "def_efg_pct" : "def_ppp";
    const values = expected
      .map((r) => r[metric])
      .filter((v): v is number => typeof v === "number");
    // Click the label: on touch widths the header's "i" button explains instead.
    await page
      .locator(`.ibpl-dt-scroll thead th[data-column="${metric}"]`)
      .click({ position: { x: 6, y: 6 } });
    await expect(
      page.locator(`.ibpl-dt-scroll td[data-column="${metric}"]`).first(),
    ).toHaveText(
      // X-13 keeps missing values last in either direction.
      values.length ? Math.min(...values).toFixed(1) : "",
    );

    // csv_export_button(): visible columns, header labels, every field quoted.
    const download = page.waitForEvent("download");
    await page.getByRole("button", { name: "Download CSV" }).click();
    const file = await download;
    expect(file.suggestedFilename()).toMatch(
      new RegExp(
        `^${euro ? "euroleague_game_logs" : "game_logs"}_${g.params.view}_\\d{8}_\\d{6}\\.csv$`,
      ),
    );
    let csv = "";
    for await (const chunk of (await file.createReadStream())!)
      csv += chunk.toString();
    const lines = csv.split("\r\n");
    expect(lines).toHaveLength(total + 1);
    expect(
      lines[0].startsWith(
        euro
          ? '"Rd","Phase","Date","Gameflow"'
          : '"GN","Game Type","Date","Gameflow"',
      ),
    ).toBe(true);
    expect(lines[1]).toMatch(/^"[^"]*"(,"[^"]*")*$/);

    expect(
      await page.evaluate(
        () => document.documentElement.scrollWidth <= innerWidth,
      ),
    ).toBe(true);
    await page.reload();
    await expect(info).toHaveText(
      `Showing 1 to ${Math.min(50, total)} of ${total.toLocaleString("en-US")} entries`,
    );
    expect(errors).toEqual([]);
  });
}

test("[X-08] version refresh and retry recover without a navigation", async ({
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
  await expect(page.locator(".shiny-output-error")).toContainText(
    "Service busy",
  );
  await page.getByRole("button", { name: "Retry", exact: true }).click();
  await expect.poll(() => calls).toBe(2);
  await expect(page.locator(".shiny-output-error")).toHaveCount(0);
  await expect(page.locator("#gl_table")).not.toHaveClass(/recalculating/);
  await page.evaluate(() => window.dispatchEvent(new Event("focus")));
  await expect.poll(() => calls).toBe(3);
});
