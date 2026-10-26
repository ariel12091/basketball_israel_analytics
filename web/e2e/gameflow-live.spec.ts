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
