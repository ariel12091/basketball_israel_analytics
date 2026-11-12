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
type Page = import("@playwright/test").Page;
const isPhone = (info: { project: { name: string } }) =>
  info.project.name === "phone";
// Phones (user 2026-11-11): one quarter at a time behind tabs, and every
// selection reports in a bottom panel that opens and closes.
const panelBar = (page: Page) => page.locator(".gf-m-panel-bar");
const setPanel = async (page: Page, open: boolean) => {
  if ((await panelBar(page).getAttribute("aria-expanded")) !== String(open))
    await panelBar(page).click();
};
const showQuarter = (page: Page, q: number) =>
  page.getByRole("tab", { name: q <= 4 ? `Q${q}` : `OT${q - 4}`, exact: true }).click();
/** Desktop's status line, or the phone panel's summary. */
const status = (page: Page, phone: boolean) =>
  phone ? panelBar(page) : page.locator(".ibpl-ribbon-pair-status");
const clearPlayers = async (page: Page, phone: boolean) => {
  if (phone) await setPanel(page, true);
  await page.getByRole("button", { name: "Clear players" }).click();
};
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
    await expect(charts).toHaveCount(1);
    for (let i = 0; i < (phone ? answer.bounds.length : 1); i++) {
      if (phone) await showQuarter(page, i + 1);
      const start = phone && i ? answer.bounds[i - 1] : 0,
        end = phone ? answer.bounds[i] : answer.bounds.at(-1)!;
      const bars = answer.bars.filter(
        (b) => b.end_elapsed > start && b.start_elapsed < end,
      );
      const labels = await charts
        .first()
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
        await expect(charts.first().locator(".ibpl-ribbon-pm")).toHaveCount(0);
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
    const phone = isPhone(info);
    const chart = page
      .locator("svg.ibpl-ribbon:not(.ibpl-gameflow-gutter)")
      .first();
    const starters = answer.bars.filter((b) => b.start_elapsed === 0);
    const a = starters[0],
      b = starters.find((b) => b.side !== a.side)!;
    const pick = async (key: string) => {
      if (phone) await setPanel(page, false);
      const p = chart
        .locator(`.ibpl-ribbon-lane[data-player-key="${key}"]`)
        .first();
      await p.click();
      await page.mouse.move(0, 0);
    };
    await pick(laneKey(a));
    if (phone) await setPanel(page, true);
    await expect(
      page.getByRole("region", { name: "Stint lineups", exact: true }),
    ).toBeVisible();
    await pick(laneKey(b));
    const windows = sharedWindows(answer.bars, [laneKey(a), laneKey(b)]),
      secs = windows.reduce((s, w) => s + w.end - w.start, 0);
    await expect(status(page, phone)).toContainText(
      phone ? `Head to head ${clock(secs)}` : `${clock(secs)} on court together`,
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
    const tripleSecs = clock(triple.reduce((sum, w) => sum + w.end - w.start, 0));
    await expect(status(page, phone)).toContainText(
      phone ? `Head to head ${tripleSecs}` : `${tripleSecs} on court together`,
    );
    if (phone) await setPanel(page, true);
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
    await clearPlayers(page, phone);
    await expect(status(page, phone)).toContainText(
      phone ? "Nothing selected" : "Select two or more",
    );
    const hit = chart.locator(".ibpl-ribbon-margin-hit");
    await hit.hover();
    await expect(page.getByRole("tooltip")).toContainText(g.case.team_name);
    if (phone) {
      // The full timeline sits behind the menu.
      await page.getByRole("button", { name: "More options" }).click();
      await page
        .getByRole("button", { name: "View full timeline", exact: true })
        .click();
      const scroller = page.locator(".ibpl-gameflow-full");
      const pin = page.locator(".ibpl-gameflow-gutter"),
        x = (await pin.boundingBox())!.x;
      await scroller.evaluate((el) => (el.scrollLeft = 350));
      expect((await pin.boundingBox())!.x).toBeCloseTo(x, 0);
      await pin.locator(".ibpl-ribbon-name").first().press("Enter");
      await expect(status(page, phone)).not.toContainText("Nothing selected");
      // A quarter tab leaves the full timeline for that one quarter.
      await showQuarter(page, 2);
      await expect(page.locator(".ibpl-ribbon-quarter-card")).toHaveCount(1);
      await expect(page.locator('[data-quarter="2"]')).toBeVisible();
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
}, info) => {
  const { answer } = await routeFixture(page, 673);
  const phone = isPhone(info);
  // A player's row selects his whole game (a bar selects one stint).
  const button = (key: string) => {
    const l = answer.lanes.find((x) => laneKey(x) === key)!;
    return page
      .locator("svg.ibpl-ribbon")
      .getByRole("button", { name: new RegExp(`^${l.player_label},`) })
      .first();
  };
  const lane = (key: string) => ({
    click: async () => {
      if (phone) await setPanel(page, false);
      await button(key).click();
      if (phone) await setPanel(page, true);
    },
    press: async (k: string) => {
      if (phone) await setPanel(page, false);
      await button(key).press(k);
      if (phone) await setPanel(page, true);
    },
  });
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
  await expect(rows.first()).toContainText(/ · Net [+-]?[\d.–]+ · ORtg [\d.–]+ · DRtg [\d.–]+/);
  await expect(card).toContainText("+/-");
  const wallace = answer.lanes.find((l) => l.player_label.includes("WALLACE"))!;
  await lane(`own:${wallace.player_key}`).click();
  await page.mouse.move(0, 0);
  await expect(page.getByRole("region", { name: "Shared minutes detail" })).toContainText("Net");
  await clearPlayers(page, phone);
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
  // At most two lineups in the tip (user 2026-11-11), each rated.
  expect(await page.locator(".gf-hover-tip-row").count()).toBeLessThanOrEqual(2);
  await expect(page.locator(".gf-hover-tip-row").first()).toContainText(/Net [+-]?[\d.–]+ · ORtg [\d.–]+ · DRtg [\d.–]+/);
});
test("EuroLeague Gameflow card shows ratings", async ({ page }, info) => {
  const { answer } = await routeFixture(page, 1147);
  const p = answer.lanes.find((l) => l.side === "own")!;
  await page
    .locator("svg.ibpl-ribbon")
    .getByRole("button", { name: new RegExp(`^${p.player_label},`) })
    .first()
    .click();
  await page.mouse.move(0, 0);
  if (isPhone(info)) await setPanel(page, true);
  await expect(page.getByRole("region", { name: `${p.player_label} game detail` })).toContainText(/ORtg[\d.]+DRtg/);
});

// Runs (shared_definitions.md, Gameflow run): a band per run over the chart
// and the rows, a chip naming it, and a tooltip that explains it.
test("Gameflow marks game 673's runs and explains the defining one", async ({
  page,
}, info) => {
  test.skip(isPhone(info), "phones list the runs in their panel (next test)");
  await routeFixture(page, 673);
  const chips = page.locator(".gf-run-chip");
  await expect(chips).toHaveCount(3);
  const defining = page.getByRole("button", {
    name: "Maccabi Tel-Aviv 37-5 run, defining stretch",
    exact: true,
  });
  await expect(defining).toHaveText("37-5 ›");
  await expect(defining).toHaveClass(/is-defining/);
  await expect(defining).toHaveClass(/is-own/);
  // The defining band crosses the Q3/Q4 line (elapsed 1800).
  const band = page.locator(".gf-run-band.is-defining").first();
  expect(Number(await band.getAttribute("data-start"))).toBeLessThan(1800);
  expect(Number(await band.getAttribute("data-end"))).toBeGreaterThan(1800);
  await defining.hover();
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
  // Chips never overlap.
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
test("Phone: the runs are listed in the panel and explained there", async ({
  page,
}, info) => {
  test.skip(!isPhone(info), "phone layout only");
  await routeFixture(page, 673);
  // The strip shades all three runs, unsplit; each quarter names its runs.
  await expect(page.locator(".gf-m-head .gf-run-band")).toHaveCount(3);
  await expect(page.locator(".gf-run-chip")).toHaveCount(1);
  await expect(page.locator(".gf-run-chip")).toHaveText("16-6 ›");
  // Q4 carries the 37-5 run over from Q3 with a "‹" chip; a tap explains it.
  await showQuarter(page, 4);
  const cont = page.locator(".gf-run-chip.is-cont");
  await expect(cont).toHaveText("‹ 37-5");
  await cont.click();
  await expect(panelBar(page)).toContainText("Maccabi Tel-Aviv 37-5 run · defining stretch");
  await expect(page.locator(".gf-m-panel-body .gf-run-tip-line").last()).toContainText("Estimated cause");
  await showQuarter(page, 1);
  await page.getByRole("button", { name: "Runs 3 ›" }).click();
  const rows = page.locator(".gf-m-runrow");
  await expect(rows).toHaveCount(3);
  await expect(rows.nth(2)).toContainText("Maccabi Tel-Aviv 37-5 · defining");
  await expect(rows.nth(2)).toContainText("Q3 2:25 – Q4 2:31");
  await rows.nth(2).click();
  // The run's quarter is shown and the panel explains it.
  await expect(page.getByRole("tab", { name: "Q3", exact: true })).toHaveAttribute("aria-selected", "true");
  await expect(panelBar(page)).toContainText("Maccabi Tel-Aviv 37-5 run · defining stretch");
  await expect(panelBar(page)).toContainText("Q3 2:25 – Q4 2:31 · from down 20 to up 12");
  const lines = await page.locator(".gf-m-panel-body .gf-run-tip-line").allTextContents();
  expect(lines[0]).toContain("since Q3 3:54");
  expect(lines.some((l) => /\d+% from three \(\d+\/\d+\).*shooting variance$/.test(l))).toBe(true);
  expect(lines.at(-1)).toMatch(/^Estimated cause/);
  await expect(page.getByRole("region", { name: "Run lineups", exact: true })).toBeVisible();
  // Back to the list, and from a run to its place on the timeline.
  await page.getByRole("button", { name: "‹ All runs" }).click();
  await expect(rows).toHaveCount(3);
  await rows.nth(1).click();
  await page.getByRole("button", { name: "Show on timeline ›" }).click();
  await expect(panelBar(page)).toHaveAttribute("aria-expanded", "false");
  await expect(page.getByRole("tab", { name: "Q2", exact: true })).toHaveAttribute("aria-selected", "true");
  await expect(page.locator('[data-quarter="2"] .gf-run-band.is-open')).toHaveCount(1);
});
test("Gameflow run edges are drawn over the bars", async ({ page }, info) => {
  await routeFixture(page, 673);
  // Solid edges where a run starts and ends, so it reads against the dark
  // lanes. A quarter card only shows the edges that fall inside it.
  await expect(page.locator(".gf-run-band").first()).toBeAttached();
  const bands = await page.locator("svg.ibpl-ribbon .gf-run-band").count(),
    edges = await page.locator(".gf-run-edge").count();
  // Phones draw one quarter (Q1 holds the 16-6 run, both its ends).
  if (info.project.name === "desktop") expect(edges).toBe(2 * bands);
  else expect(edges).toBe(2);
});
test("Together comes first and every card fits without scrolling", async ({
  page,
}, info) => {
  const { answer } = await routeFixture(page, 673);
  const row = (label: string) =>
    page
      .locator("svg.ibpl-ribbon")
      .getByRole("button", { name: new RegExp(`^${label},`) })
      .first();
  for (const l of ["JOHN DIBARTOLOMEO", "KEATON WALLACE"]) {
    await row(l).click();
    await page.mouse.move(0, 0);
  }
  if (isPhone(info)) await setPanel(page, true);
  const cards = page.locator(".gf-player-cards > section");
  await expect(cards).toHaveCount(3);
  await expect(cards.first()).toHaveAttribute("aria-label", "Shared minutes detail");
  // The card column never hides a card behind its own scroll.
  const [scroll, client] = await page
    .locator(".gf-player-cards")
    .evaluate((el) => [el.scrollHeight, el.clientHeight]);
  expect(scroll).toBeLessThanOrEqual(client + 1);
  if (info.project.name === "desktop") {
    // No empty frame under the lineups: the box hugs its rows.
    const box = page.getByRole("region", { name: "Shared lineups", exact: true });
    const last = box.getByRole("button").last();
    const [b, l] = [(await box.boundingBox())!, (await last.boundingBox())!];
    expect(b.y + b.height - (l.y + l.height)).toBeLessThan(24);
    // And Together is on screen without scrolling the page.
    const t = (await cards.first().boundingBox())!;
    expect(t.y + t.height).toBeLessThanOrEqual(900);
  }
  void answer;
});
test("Phone: a swipe across the quarter changes quarter", async ({ page }, info) => {
  test.skip(!isPhone(info), "phone layout only");
  await routeFixture(page, 673);
  const card = page.locator(".ibpl-ribbon-quarter-card");
  const swipe = async (dx: number) => {
    const b = (await card.boundingBox())!;
    const x = b.x + b.width / 2, y = b.y + 120;
    await page.mouse.move(x, y);
    await page.mouse.down();
    await page.mouse.move(x + dx / 2, y + 4);
    await page.mouse.move(x + dx, y + 6);
    await page.mouse.up();
  };
  const tab = (q: number) => page.getByRole("tab", { name: `Q${q}`, exact: true });
  await swipe(-160);
  await expect(tab(2)).toHaveAttribute("aria-selected", "true");
  // The swipe selected nobody, though it started on a bar.
  await expect(panelBar(page)).toContainText("Nothing selected");
  await swipe(-160);
  await expect(tab(3)).toHaveAttribute("aria-selected", "true");
  await swipe(160);
  await expect(tab(2)).toHaveAttribute("aria-selected", "true");
  // Nothing before Q1.
  await swipe(160);
  await swipe(160);
  await expect(tab(1)).toHaveAttribute("aria-selected", "true");
});
test("Phone: a tap outside the run details closes them", async ({ page }, info) => {
  test.skip(!isPhone(info), "phone layout only");
  await routeFixture(page, 673);
  await page.getByRole("button", { name: "Runs 3 ›" }).click();
  await page.locator(".gf-m-runrow").nth(2).click();
  await expect(panelBar(page)).toContainText("Maccabi Tel-Aviv 37-5 run");
  await expect(panelBar(page)).toHaveAttribute("aria-expanded", "true");
  // A tap just above the open panel: the run closes, and the tap selects no
  // player.
  const above = async () => {
    const p = (await page.locator(".gf-m-panel").boundingBox())!;
    await page.mouse.click(200, p.y - 8);
  };
  await above();
  await expect(panelBar(page)).toHaveAttribute("aria-expanded", "false");
  await expect(panelBar(page)).toContainText("Nothing selected");
  // The runs list closes the same way.
  await page.getByRole("button", { name: "Runs 3 ›" }).click();
  await expect(page.locator(".gf-m-runrow")).toHaveCount(3);
  await above();
  await expect(page.locator(".gf-m-runrow")).toHaveCount(0);
});
test("Phone: the score strip names which team is ahead on each side", async ({
  page,
}, info) => {
  test.skip(!isPhone(info), "phone layout only");
  await routeFixture(page, 673);
  const strip = page.locator(".gf-m-head .ibpl-ribbon-overview-svg");
  const own = strip.locator(".gf-m-side.is-own"),
    opp = strip.locator(".gf-m-side.is-opp");
  await expect(own).toHaveText("Maccabi Tel-Aviv");
  await expect(opp).toHaveText("Hapoel Tel-Aviv");
  // Up is the team on top: its label sits above the other's.
  const [o, p] = [(await own.boundingBox())!, (await opp.boundingBox())!];
  expect(o.y).toBeLessThan(p.y);
});
test("Phone: a tap on the score strip opens a run, or the quarter tapped", async ({
  page,
}, info) => {
  test.skip(!isPhone(info), "phone layout only");
  await routeFixture(page, 673);
  const strip = page.locator(".gf-m-head .ibpl-ribbon-overview-svg");
  const box = (await strip.boundingBox())!;
  const at = (elapsed: number) => page.mouse.click(box.x + (elapsed / 2400) * box.width, box.y + box.height / 2);
  // Inside the 37-5 run (Q3 2:25 – Q4 2:31): the run opens, on its quarter.
  await at(2000);
  await expect(panelBar(page)).toContainText("Maccabi Tel-Aviv 37-5 run · defining stretch");
  await expect(page.getByRole("tab", { name: "Q3", exact: true })).toHaveAttribute("aria-selected", "true");
  // Outside every run (Q2 1:40): that quarter, nothing selected.
  await setPanel(page, false);
  await at(800);
  await expect(page.getByRole("tab", { name: "Q2", exact: true })).toHaveAttribute("aria-selected", "true");
  await expect(panelBar(page)).toContainText("Nothing selected");
});
test("Phone: the panel sums up a selection on screen; short lineup list", async ({
  page,
}, info) => {
  test.skip(!isPhone(info), "phone layout only");
  await routeFixture(page, 673);
  const row = (label: string) =>
    page.locator("svg.ibpl-ribbon").getByRole("button", { name: new RegExp(`^${label},`) }).first();
  await expect(panelBar(page)).toContainText("Nothing selected");
  await row("JOHN DIBARTOLOMEO").click();
  await expect(panelBar(page)).toContainText("JOHN DIBARTOLOMEO · game");
  await row("KEATON WALLACE").click();
  await expect(panelBar(page)).toContainText(/Together 12:59 · \+\/- \+30 · Net \+115\.4/);
  // Pinned to the bottom of the screen, collapsed until tapped.
  const b = (await panelBar(page).boundingBox())!;
  expect(b.y + b.height).toBeLessThanOrEqual(844);
  expect(b.y).toBeGreaterThan(740);
  await expect(page.locator(".gf-player-cards")).toHaveCount(0);
  await setPanel(page, true);
  await expect(page.locator(".gf-player-cards")).toBeInViewport();
  const list = page.getByRole("region", { name: "Shared lineups", exact: true });
  await expect(list.locator(".ibpl-ribbon-shared-lineup")).toHaveCount(3);
  await list.getByRole("button", { name: /^Show all \d+ lineups$/ }).click();
  expect(await list.locator(".ibpl-ribbon-shared-lineup").count()).toBeGreaterThan(3);
});
test("A run's band lists both teams' lineups inside the run", async ({ page }, info) => {
  const { answer } = await routeFixture(page, 673);
  const phone = isPhone(info);
  const run = answer.runs.find((r) => r.defining)!;
  if (phone) await showQuarter(page, 3);
  const band = page
    .locator(`svg.ibpl-ribbon .gf-run-band[data-start="${run.start}"]`)
    .first();
  await band.scrollIntoViewIfNeeded();
  // Its top strip, above the first lane row: no bar covers it there.
  const box = (await band.boundingBox())!;
  const click = () => page.mouse.click(box.x + box.width / 2, box.y + 4);
  if (info.project.name === "desktop") {
    await page.mouse.move(box.x + box.width / 2, box.y + 4);
    await expect(page.getByRole("tooltip")).toHaveCount(0);
  }
  await click();
  const list = page.getByRole("region", { name: "Run lineups", exact: true });
  await expect(list).toBeVisible();
  await expect(status(page, phone)).toContainText("Maccabi Tel-Aviv 37-5");
  // Both teams, the run team first.
  await expect(list.locator(".ibpl-ribbon-detail-list")).toHaveCount(2);
  const heads = await list.locator(".ibpl-ribbon-detail-head").allTextContents();
  expect(heads.slice(1)).toEqual(["Maccabi Tel-Aviv", "Hapoel Tel-Aviv"]);
  await expect(list.locator(".ibpl-ribbon-shared-lineup").first()).toContainText("in the run");
  // Windows read as the run's own does: period and game clock (user 2026-11-12).
  await expect(list.locator(".ibpl-ribbon-shared-lineup").first()).toContainText(/Q\d \d+:\d\d – Q\d \d+:\d\d/);
  // The chart holding the run (a phone's quarter card, or the one chart).
  const chart = page
    .locator("svg.ibpl-ribbon:not(.ibpl-gameflow-gutter)")
    .filter({ has: page.locator(`.gf-run-band[data-start="${run.start}"]`) })
    .first();
  // The score line shows the run's stretch picked out from the rest.
  await expect(chart).toHaveClass(/is-run-focused/);
  await expect(chart.locator(".ibpl-ribbon-margin-focus").first()).toHaveAttribute("clip-path", /url\(/);
  // A lineup clicked in the list is marked only inside the run.
  await list.locator(".ibpl-ribbon-shared-lineup").first().click();
  const marks = await chart.locator(".ibpl-ribbon-lineup-mark").evaluateAll((els) =>
    els.map((e) => [Number(e.getAttribute("x")), Number(e.getAttribute("width"))]),
  );
  expect(marks.length).toBeGreaterThan(0);
  const runBox = await chart.locator(`.gf-run-band[data-start="${run.start}"]`).first().evaluate((e) => [
    Number(e.getAttribute("x")),
    Number(e.getAttribute("width")),
  ]);
  for (const [x, w] of marks) {
    expect(x).toBeGreaterThanOrEqual(runBox[0] - 0.5);
    expect(x + w).toBeLessThanOrEqual(runBox[0] + runBox[1] + 0.5);
  }
  await list.locator(".ibpl-ribbon-shared-lineup").first().click();
  // The same band again closes it (measured afresh: the page may have moved).
  if (phone) await setPanel(page, false);
  await band.scrollIntoViewIfNeeded();
  const again = (await band.boundingBox())!;
  await page.mouse.click(again.x + again.width / 2, again.y + 4);
  await expect(chart).not.toHaveClass(/is-run-focused/);
  if (!phone) await expect(list).toHaveCount(0);
});
test("Phone: every quarter shows its whole chart", async ({ page }, info) => {
  test.skip(info.project.name !== "phone", "quarter cards are phone-only");
  await routeFixture(page, 673);
  for (const q of [1, 2, 3, 4]) {
    await showQuarter(page, q);
    const [frame, svg] = await page
      .locator(`[data-quarter="${q}"] .ibpl-ribbon-quarter-frame`)
      .evaluate((f) => [
        f.getBoundingClientRect().right,
        f.querySelector("svg.ibpl-ribbon")!.getBoundingClientRect().right,
      ]);
    // The chart ends inside its frame: nothing is cut off at the right.
    expect(svg).toBeLessThanOrEqual(frame + 0.5);
  }
});
test("Gameflow draws no run in a game without one", async ({ page }) => {
  await routeFixture(page, 440);
  await expect(page.locator("svg.ibpl-ribbon").first()).toBeVisible();
  await expect(page.locator(".gf-run-chip")).toHaveCount(0);
  await expect(page.locator(".gf-run-band")).toHaveCount(0);
  await expect(page.getByRole("button", { name: /^Runs / })).toHaveCount(0);
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
