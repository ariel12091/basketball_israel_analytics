import { test, expect } from "@playwright/test";
import {
  gameflowFiles,
  loadGameflow,
  loadFixture,
  GAMEFLOW_FIXTURES,
} from "../test/gameflowGoldens";
import {
  windowDetail,
  sharedWindows,
  clock,
  laneKey,
  ratingLabel,
} from "../src/rules/gameflow";
import { stintRatings } from "../src/rules/gameflowRatings";
import { rRound } from "../src/rules/round";
for (const file of gameflowFiles)
  test(`Gameflow ${file}`, async ({ page }, info) => {
    const { g, answer, url } = loadGameflow(file),
      errors: string[] = [];
    page.on("pageerror", (e) => errors.push(e.message));
    await page.route("**/api/v1/**", (route) =>
      route.fulfill({
        json: new URL(route.request().url()).pathname.endsWith("/version")
          ? { leagues: [{ league: g.case.league, version: "golden" }] }
          : answer,
      }),
    );
    await page.goto(url);
    const charts = page.locator("svg.ibpl-ribbon:not(.ibpl-gameflow-gutter)");
    const phone = info.project.name === "phone";
    await expect(charts).toHaveCount(phone ? answer.bounds.length : 1);
    for (let i = 0; i < (phone ? answer.bounds.length : 1); i++) {
      const start = phone && i ? answer.bounds[i - 1] : 0,
        end = phone ? answer.bounds[i] : answer.bounds.at(-1)!;
      const bars = answer.bars.filter(
        (b) => b.end_elapsed > start && b.start_elapsed < end,
      );
      const labels = await charts
        .nth(i)
        .locator(".ibpl-ribbon-lane")
        .evaluateAll((nodes) => nodes.map((n) => n.getAttribute("aria-label")));
      expect(labels).toEqual(
        bars.map(
          (b) =>
            windowDetail(
              answer.lanes,
              answer.steps,
              answer.dict,
              b,
              Math.max(start, b.start_elapsed),
              Math.min(end, b.end_elapsed),
            ).label,
        ),
      );
      if (phone)
        await expect(charts.nth(i).locator(".ibpl-ribbon-pm")).toHaveCount(0);
    }
    if (answer.health)
      await expect(page.getByRole("note")).toHaveText(answer.health);
    expect(
      await page.evaluate(
        () => document.documentElement.scrollWidth <= innerWidth + 1,
      ),
    ).toBe(true);
    if (file === "il_regular.json" || file === "euroleague_overtime.json")
      await page.screenshot({
        path: `../output/playwright/gameflow-${file}-${info.project.name}.png`,
        fullPage: true,
      });
    expect(errors).toEqual([]);
  });

for (const league of ["il", "euroleague", "eurocup"] as const)
  test(`Gameflow selection, lineup focus and score ${league}`, async ({
    page,
  }, info) => {
    const { g, answer, url } = loadGameflow(`${league}_regular.json`);
    await page.route("**/api/v1/**", (r) =>
      r.fulfill({
        json: new URL(r.request().url()).pathname.endsWith("/version")
          ? { leagues: [{ league, version: "golden" }] }
          : answer,
      }),
    );
    await page.goto(url);
    const chart = page
      .locator("svg.ibpl-ribbon:not(.ibpl-gameflow-gutter)")
      .first();
    const starters = answer.bars.filter((b) => b.start_elapsed === 0);
    const a = starters[0],
      b = starters.find((b) => b.side !== a.side)!;
    const pick = async (key: string) => {
      const p = chart
        .locator(`.ibpl-ribbon-lane[data-player-key="${key}"]`)
        .first();
      await p.click();
      await page.mouse.move(0, 0);
    };
    await pick(laneKey(a));
    await expect(
      page.getByRole("region", { name: "Stint lineups", exact: true }),
    ).toBeVisible();
    await pick(laneKey(b));
    const windows = sharedWindows(answer.bars, [laneKey(a), laneKey(b)]),
      secs = windows.reduce((s, w) => s + w.end - w.start, 0);
    await expect(page.locator(".ibpl-ribbon-pair-status")).toContainText(
      `${clock(secs)} on court together`,
    );
    const third = starters.find(
      (p) => laneKey(p) !== laneKey(a) && laneKey(p) !== laneKey(b),
    )!;
    await pick(laneKey(third));
    const triple = sharedWindows(answer.bars, [
      laneKey(a),
      laneKey(b),
      laneKey(third),
    ]);
    await expect(page.locator(".ibpl-ribbon-pair-status")).toContainText(
      `${clock(triple.reduce((sum, w) => sum + w.end - w.start, 0))} on court together`,
    );
    const groups = page.getByRole("region", {
      name: "Shared lineups",
      exact: true,
    });
    await expect(groups).toBeVisible();
    await expect(groups.locator(".ibpl-ribbon-detail-side")).toHaveCount(0);
    await groups.getByRole("button").first().click();
    await expect(groups.getByRole("button").first()).toHaveAttribute(
      "aria-pressed",
      "true",
    );
    await expect(
      page.locator(".ibpl-ribbon-lineup-mark").first(),
    ).toBeAttached();
    await page.getByRole("button", { name: "Clear players" }).click();
    await expect(page.locator(".ibpl-ribbon-pair-status")).toContainText(
      "Select two or more",
    );
    const hit = chart.locator(".ibpl-ribbon-margin-hit");
    await hit.hover();
    await expect(page.getByRole("tooltip")).toContainText(g.case.team_name);
    if (info.project.name === "phone") {
      await page
        .getByRole("button", { name: "View full timeline", exact: true })
        .click();
      const scroller = page.locator(".ibpl-gameflow-full");
      const pin = page.locator(".ibpl-gameflow-gutter"),
        x = (await pin.boundingBox())!.x;
      await scroller.evaluate((el) => (el.scrollLeft = 350));
      expect((await pin.boundingBox())!.x).toBeCloseTo(x, 0);
      await pin.locator(".ibpl-ribbon-name").first().press("Enter");
      await expect(page.locator(".ibpl-ribbon-pair-status")).not.toContainText(
        "Select two or more",
      );
      await page.getByRole("button", { name: "Jump to Q2" }).click();
      await expect(page.locator(".ibpl-ribbon-quarter-card")).toHaveCount(
        answer.bounds.length,
      );
    }
  });

// Stint ratings (shared_definitions.md, Gameflow stint rating), from the
// committed reader fixtures: Net/ORtg/DRtg on the card (no possession count,
// user 2026-11-11), Net in a bar's hover tip and on each lineup row
// and Net in a bar's hover tip.
const routeFixture = async (
  page: import("@playwright/test").Page,
  game: keyof typeof GAMEFLOW_FIXTURES,
) => {
  const f = loadFixture(game);
  await page.route("**/api/v1/**", (r) =>
    r.fulfill({
      json: new URL(r.request().url()).pathname.endsWith("/version")
        ? { leagues: [{ league: f.league, version: "golden" }] }
        : f.answer,
    }),
  );
  await page.goto(f.url);
  return f;
};
const signed = (v: number | null) =>
  v != null && rRound(v, 1) > 0 ? `+${ratingLabel(v)}` : ratingLabel(v);
test("Gameflow card shows Net, ORtg and DRtg", async ({
  page,
}) => {
  const { answer } = await routeFixture(page, 673);
  // A player's row selects his whole game (a bar selects one stint).
  const lane = (key: string) => {
    const l = answer.lanes.find((x) => laneKey(x) === key)!;
    return page
      .locator("svg.ibpl-ribbon")
      .getByRole("button", { name: new RegExp(`^${l.player_label},`) })
      .first();
  };
  const dib = answer.lanes.find((l) => l.player_label.includes("DIBARTOLOMEO"))!;
  const r = stintRatings(answer.events, "own", [dib.player_key]);
  expect(r.offPoss).toBe(47);
  await lane(`own:${dib.player_key}`).click();
  await page.mouse.move(0, 0);
  const card = page.getByRole("region", { name: `${dib.player_label} game detail` });
  await expect(card).toContainText(`Net${signed(r.net)}ORtg${ratingLabel(r.ortg)}DRtg${ratingLabel(r.drtg)}`);
  await expect(card).not.toContainText("poss");
  // Each lineup row of his game carries its Net too.
  const rows = page.getByRole("region", { name: "Stint lineups", exact: true }).getByRole("button");
  await expect(rows.first()).toContainText(/ · Net [+-]?[\d.]+|· Net –/);
  await expect(card).toContainText("+/-");
  const wallace = answer.lanes.find((l) => l.player_label.includes("WALLACE"))!;
  await lane(`own:${wallace.player_key}`).click();
  await page.mouse.move(0, 0);
  await expect(page.getByRole("region", { name: "Shared minutes detail" })).toContainText("Net");
  await page.getByRole("button", { name: "Clear players" }).click();
  const opp = answer.lanes.find((l) => l.side === "opp")!;
  await lane(`own:${dib.player_key}`).click();
  // The card opens between the teams and moves the opponent's chart down.
  // Enter, not a click: with a card open the opponent's name buttons report a
  // box under the card panel (seen on the stored goldens too, before ratings).
  await expect(card).toBeVisible();
  await lane(`opp:${opp.player_key}`).press("Enter");
  await page.mouse.move(0, 0);
  const h2h = page.getByRole("region", { name: "Head to head detail" });
  await expect(h2h).toBeVisible();
  await expect(h2h).not.toContainText("Net");
});
test("Gameflow bar tooltip adds Net after the +/-", async ({ page }, info) => {
  test.skip(info.project.name === "phone", "hover tips are pointer-only");
  const { answer } = await routeFixture(page, 673);
  const dib = answer.bars.find((b) => b.player_label.includes("DIBARTOLOMEO"))!;
  const bar = page
    .locator("svg.ibpl-ribbon:not(.ibpl-gameflow-gutter)")
    .first()
    .locator(`.ibpl-ribbon-lane[data-player-key="own:${dib.player_key}"]`)
    .first();
  // The tip waits for a real pointer movement (it ignores a bar the pointer
  // merely rests on after a re-render).
  const box = (await bar.boundingBox())!;
  await page.mouse.move(box.x + box.width / 2 - 2, box.y + box.height / 2);
  await page.mouse.move(box.x + box.width / 2, box.y + box.height / 2);
  await expect(page.getByRole("tooltip")).toContainText(/\+\/- [+-]?\d+ · Net [+-]?[\d.–]+ · /);
  await expect(page.getByRole("tooltip")).not.toContainText("poss");
  // At most two lineups in the tip (user 2026-11-11).
  expect(await page.locator(".gf-hover-tip-row").count()).toBeLessThanOrEqual(2);
});
test("EuroLeague Gameflow card shows ratings", async ({ page }) => {
  const { answer } = await routeFixture(page, 1147);
  const p = answer.lanes.find((l) => l.side === "own")!;
  await page
    .locator("svg.ibpl-ribbon")
    .getByRole("button", { name: new RegExp(`^${p.player_label},`) })
    .first()
    .click();
  await page.mouse.move(0, 0);
  await expect(page.getByRole("region", { name: `${p.player_label} game detail` })).toContainText(/ORtg[\d.]+DRtg/);
});

// Runs (shared_definitions.md, Gameflow run): a band per run over the chart
// and the rows, a chip naming it, and a tooltip that explains it.
test("Gameflow marks game 673's runs and explains the defining one", async ({
  page,
}, info) => {
  await routeFixture(page, 673);
  const chips = page.locator(".gf-run-chip");
  await expect(chips).toHaveCount(3);
  const defining = page.getByRole("button", {
    name: "Maccabi Tel-Aviv 37-5 run, defining stretch",
  });
  await expect(defining).toHaveText("37-5 ›");
  await expect(defining).toHaveClass(/is-defining/);
  await expect(defining).toHaveClass(/is-own/);
  // The defining band crosses the Q3/Q4 line (elapsed 1800).
  const band = page.locator(".gf-run-band.is-defining").first();
  expect(Number(await band.getAttribute("data-start"))).toBeLessThan(1800);
  expect(Number(await band.getAttribute("data-end"))).toBeGreaterThan(1800);
  if (info.project.name === "phone") await defining.click();
  else await defining.hover();
  const tip = page.getByRole("tooltip").filter({ hasText: "Estimated cause" });
  await expect(tip).toBeVisible();
  const lines = await tip.locator(".gf-run-tip-line").allTextContents();
  expect(lines[0]).toContain("Maccabi Tel-Aviv 37-5 · Q3 2:25 – Q4 2:31");
  expect(lines[1]).toContain("since Q3 3:54");
  expect(lines[1]).toContain("DIBARTOLOMEO");
  const three = lines.findIndex((l) => l.includes("from three"));
  expect(three).toBeGreaterThan(2);
  expect(lines[three]).toMatch(/\d+% from three \(\d+\/\d+\), \d+% the rest of the game \(\d+\/\d+\) — shooting variance$/);
  expect(lines.at(-1)).toMatch(/^Estimated cause/);
  if (info.project.name === "phone") {
    // A second tap closes the pinned tip; so does a tap elsewhere.
    await defining.click();
    await expect(tip).toHaveCount(0);
    await defining.click();
    await expect(tip).toBeVisible();
    await page.getByRole("button", { name: "Clear players" }).click();
    await expect(tip).toHaveCount(0);
  }
  // Chips never overlap, at either width.
  const boxes = await chips.evaluateAll((els) =>
    els.map((e) => e.getBoundingClientRect()).map((r) => [r.left, r.right, r.top, r.bottom]),
  );
  for (let i = 0; i < boxes.length; i++)
    for (let j = i + 1; j < boxes.length; j++) {
      const [a, b] = [boxes[i], boxes[j]];
      const overlap = a[0] < b[1] && b[0] < a[1] && a[2] < b[3] && b[2] < a[3];
      expect(overlap).toBe(false);
    }
});
test("Gameflow draws no run in a game without one", async ({ page }) => {
  await routeFixture(page, 440);
  await expect(page.locator("svg.ibpl-ribbon").first()).toBeVisible();
  await expect(page.locator(".gf-run-chip")).toHaveCount(0);
  await expect(page.locator(".gf-run-band")).toHaveCount(0);
});

test("Gameflow errors can be retried", async ({ page }) => {
  const { answer, url } = loadGameflow("il_regular.json");
  let failed = true;
  await page.route("**/api/v1/**", (r) =>
    !new URL(r.request().url()).pathname.endsWith("/gameflow")
      ? r.fulfill({ json: { leagues: [] } })
      : failed
        ? r.fulfill({
            status: 503,
            json: { error: "Temporarily unavailable" },
          })
        : r.fulfill({ json: answer }),
  );
  await page.goto(url);
  await expect(
    page.getByRole("alert").filter({ hasText: "Temporarily unavailable" }),
  ).toBeVisible();
  failed = false;
  await page.getByRole("button", { name: "Retry", exact: true }).click();
  await expect(page.locator("svg.ibpl-ribbon").first()).toBeVisible();
});
