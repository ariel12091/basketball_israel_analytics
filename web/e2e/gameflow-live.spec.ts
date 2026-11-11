import { test, expect } from "./preview";
test.skip(
  process.env.LIVE_DB_SMOKE !== "1",
  "Opt-in live read-only SSR checks",
);
test.use({ extraHTTPHeaders: {} });
// The `<league>_regular.json` Gameflow golden cases, written out so the live
// check runs without the (gitignored) goldens, e.g. in CI.
const CASES = {
  il: { game: 390, team: 2, ownTeam: "Maccabi Tel-Aviv", season: "2026" },
  euroleague: { game: 781, team: 8, ownTeam: "Olympiacos Piraeus", season: "2025" },
  eurocup: { game: 671, team: 966, ownTeam: "Cosea JL Bourg-en-Bresse", season: "2025" },
} as const;
for (const league of ["il", "euroleague", "eurocup"] as const)
  test(`Gameflow initial server HTML ${league}`, async ({ page }) => {
    const c = CASES[league];
    const answer = { meta: { ownTeam: c.ownTeam, season: c.season } };
    const response = await page.goto(
      `/${league}/gameflow?game_id=${c.game}&team_id=${c.team}`,
    );
    const html = await response!.text();
    expect(html).toContain('class="ibpl-ribbon');
    expect(html).toContain(answer.meta.ownTeam);
    await expect(page.locator("svg.ibpl-ribbon").first()).toBeVisible();
    await expect(page.locator(".ibpl-ribbon-pair-status")).toContainText(
      "Select two or more",
    );
    await expect(
      page.getByRole("link", { name: "Back to Game Logs" }),
    ).toHaveAttribute(
      "href",
      `/${league}/game-logs?season=${answer.meta.season}`,
    );
    expect(
      await page.evaluate(
        () => document.documentElement.scrollWidth <= innerWidth + 1,
      ),
    ).toBe(true);
  });

// Runs and stint ratings (shared_definitions.md, Gameflow stint rating and
// Gameflow run) on live data: both readers return credited events and
// possession ends, and game 673 shows its defining run and a rated card.
for (const [league, game, team] of [
  ["il", 673, 2],
  ["euroleague", 781, 8],
] as const)
  test(`Gameflow events and ratings live ${league}`, async ({ page }) => {
    // Fetched from the page so a protected Preview's bypass header applies.
    await page.goto(`/${league}/gameflow?game_id=${game}&team_id=${team}`);
    const body = await page.evaluate(async (url) => {
      const r = await fetch(url);
      const j = await r.json();
      return { ok: r.ok, events: j.events?.length ?? 0, ratings: j.ratings != null };
    }, `/api/v1/${league}/gameflow?game_id=${game}&team_id=${team}`);
    expect(body).toEqual({ ok: true, events: expect.any(Number), ratings: true });
    expect(body.events).toBeGreaterThan(0);
  });
test("Gameflow 673 shows the defining run and a rated card live", async ({
  page,
}) => {
  await page.goto("/il/gameflow?game_id=673&team_id=2");
  await expect(
    page.getByRole("button", { name: /37-5 run, defining stretch$/ }),
  ).toHaveText("37-5 ›");
  // The chart arrives in the server HTML, before React has hydrated it: a tap
  // that early is lost, so retry until the selection shows.
  await expect(async () => {
    await page
      .locator("svg.ibpl-ribbon")
      .getByRole("button", { name: /^john dibartolomeo,/i })
      .first()
      .click();
    await expect(page.locator(".ibpl-ribbon-pair-status")).not.toContainText(
      "Select two or more",
      { timeout: 2000 },
    );
  }).toPass({ timeout: 20000 });
  await expect(
    page.getByRole("region", { name: /dibartolomeo game detail/i }),
  ).toContainText(/Net[+-]?[\d.]+ORtg/);
});
