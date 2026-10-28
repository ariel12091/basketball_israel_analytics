import { readFileSync } from "node:fs";
import { test, expect, type Page, type Route } from "@playwright/test";
import { openNavMenu } from "./navMenu";
import type { HomeFrames } from "../src/rules/home";
import type { Row } from "../src/rules/gameLogs";
import type { HomePayload } from "../src/server/homeRepo";
import ui from "../src/data/homeUi.json";
import shiny from "../src/data/shinyUi.json";
import { loadGameflow } from "../test/gameflowGoldens";
import { loadLineup } from "../test/lineupGoldens";
type Golden = {
  season: number;
  team: string;
  frames: HomeFrames;
  expected: {
    identity: { row: Row; n_teams: number } | null;
    players: Row[] | null;
    scorer: Row | null;
    lineups: { best: Row; worst: Row } | null;
    storylines: { id: string; text: string; preset: string }[];
    lastGame: { result: string; context: string } | null;
  };
};
const cases = JSON.parse(
  readFileSync("test/goldens/home/cases.json", "utf8"),
) as Record<string, Golden>;
function payload(
  g: Golden,
  season = String(g.season),
  team = g.team,
): HomePayload {
  return {
    frames: g.frames,
    meta: {
      version: "golden",
      season,
      teamId: team,
      source: "combined",
      errors: [],
    },
  };
}
/** "See more" holds every hub block but the team ratings card. */
async function openMore(page: Page) {
  const toggle = page.getByRole("button", {
    name: new RegExp(ui.text.seeMore),
  });
  await expect(toggle).toHaveAttribute("aria-expanded", "false");
  await toggle.click();
  await expect(
    page.getByRole("button", { name: ui.text.seeLess, exact: true }),
  ).toHaveAttribute("aria-expanded", "true");
}
async function routes(
  page: Page,
  handler?: (route: Route) => Promise<unknown>,
) {
  await page.route("**/api/v1/**", async (route) => {
    const u = new URL(route.request().url());
    if (u.pathname.endsWith("/version"))
      return route.fulfill({
        json: {
          leagues: [
            { league: "il", version: "golden" },
            { league: "euroleague", version: "golden" },
            { league: "eurocup", version: "golden" },
          ],
        },
      });
    if (u.pathname.endsWith("/gameflow"))
      return route.fulfill({ json: loadGameflow("il_regular.json").answer });
    if (handler) return handler(route);
    const season = u.searchParams.get("season")!,
      team = u.searchParams.get("team_id")!;
    return route.fulfill({
      json: payload(cases[`${season}_${team}`] ?? cases["empty"], season, team),
    });
  });
}
async function pick(page: Page, id: string, label: string) {
  await page.locator(`[data-input-id="${id}"] .selectize-input`).click();
  await page
    .locator(`[data-input-id="${id}"]`)
    .getByRole("option", { name: label, exact: true })
    .click();
}
const fixed = (x: unknown, signed = false) =>
  `${signed && Number(x) >= 0 ? "+" : ""}${Number(x).toFixed(1)}`;
for (const name of [
  "2026_2",
  "2026_5",
  "2027_2",
  "2027_5",
  "empty",
  "below_threshold",
  "ties",
  "one_lineup",
  "missing_values",
])
  test(`Home matches recorded R ${name}`, async ({ page }, info) => {
    const g = cases[name],
      errors: string[] = [];
    page.on("pageerror", (e) => errors.push(e.message));
    await routes(page, (route) => route.fulfill({ json: payload(g) }));
    await page.goto(`/il/home?season=${g.season}&team_id=${g.team}`);
    await expect(page.locator(".home-identity")).toHaveAttribute(
      "aria-busy",
      "false",
    );
    await expect(page.locator("#team_hub_section")).toHaveCount(0);
    await openMore(page);
    if (g.expected.identity) {
      const r = g.expected.identity.row;
      await expect(page.locator("#hub_identity .card-title")).toHaveText(
        String(r.team_name),
      );
      await expect(page.locator("#hub_identity .hub-record")).toHaveText(
        `${r.wins}-${r.losses}`,
      );
      await expect(page.locator("#hub_identity .hub-stat-value")).toHaveText([
        fixed(r.off_ppp),
        fixed(r.def_ppp),
        fixed(r.net_rtg, true),
      ]);
    } else await expect(page.locator("#hub_identity .card")).toHaveCount(0);
    await expect(page.locator("#hub_players .hub-player-name")).toHaveText(
      (g.expected.players ?? []).map(
        (p) => `${p["First Name"]} ${p["Last Name"]}`,
      ),
    );
    if (g.expected.scorer)
      await expect(page.locator("#hub_players .hub-footnote")).toHaveText(
        `Top scorer (min 3 games): ${g.expected.scorer.Player} -- ${fixed(g.expected.scorer.ppg)} ppg`,
      );
    else
      await expect(page.locator("#hub_players .hub-footnote")).toHaveCount(0);
    if (g.expected.lineups)
      await expect(page.locator("#hub_lineups .hub-lineup-players")).toHaveText(
        [
          String(g.expected.lineups.best.player_names_str),
          String(g.expected.lineups.worst.player_names_str),
        ],
      );
    else await expect(page.locator("#hub_lineups .hub-lineup")).toHaveCount(0);
    await expect(page.locator("#hub_storylines .hub-story-line")).toHaveText(
      g.expected.storylines.map((l) => l.text),
    );
    if (g.expected.lastGame) {
      await expect(page.locator("#hub_last_game")).toContainText(
        g.expected.lastGame.result,
      );
      await expect(page.locator("#hub_last_game")).toContainText(
        g.expected.lastGame.context,
      );
    }
    for (const line of g.expected.storylines) {
      await expect(
        page
          .locator("#hub_storylines .hub-story-line")
          .filter({ hasText: line.text }),
      ).toHaveAttribute(
        "href",
        line.preset
          ? `/il/compare?season=${g.season}&preset=${line.preset}&team_id=${g.team}&open_detail=true`
          : `/il/team-ratings?season=${g.season}`,
      );
    }
    await expect(page.locator(".home-nav-card")).toHaveCount(6);
    expect(
      await page.evaluate(
        () => document.documentElement.scrollWidth <= innerWidth + 1,
      ),
    ).toBe(true);
    // One compact block, 640px at most, at every width.
    expect(
      (await page.locator(".home-compact").boundingBox())!.width,
    ).toBeLessThanOrEqual(640);
    if (name === "2026_2")
      await page.screenshot({
        path: `../output/playwright/home-${info.project.name}.png`,
        fullPage: true,
      });
    expect(errors).toEqual([]);
  });
for (const league of ["euroleague", "eurocup"])
  test(`Home ${league} has only its five navigation cards`, async ({
    page,
  }) => {
    const calls: string[] = [];
    await routes(page, async (route) => {
      calls.push(route.request().url());
      return route.fulfill({ json: {} });
    });
    await page.goto(`/${league}/home?season=2025`);
    await expect(page.locator(".home-nav-card")).toHaveCount(5);
    await expect(page.locator("#team_hub_section")).toHaveCount(0);
    await expect(page.locator("#home_team")).toHaveCount(0);
    expect(calls).toEqual([]);
    await expect(page.locator(".league-chooser")).toHaveCount(0);
    await expect(page.locator(".home-more-toggle")).toHaveCount(0);
    const next = league === "euroleague" ? "eurocup" : "euroleague";
    if (page.viewportSize()!.width < 1000)
      await page.getByRole("button", { name: "Toggle navigation" }).click();
    const current = page.getByRole("button", { name: /^League:/ });
    await expect(current).toHaveText(
      `League: ${league === "eurocup" ? "EuroCup" : "EuroLeague"}`,
    );
    await openNavMenu(page, /^League:/);
    const toggle = page.locator("#league_menu");
    await expect(
      toggle.getByRole("link", { name: "Israeli League", exact: true }),
    ).toHaveAttribute("href", "/il/home");
    await toggle
      .getByRole("link", {
        name: next === "eurocup" ? "EuroCup" : "EuroLeague",
        exact: true,
      })
      .click();
    await expect(page).toHaveURL(new RegExp(`/${next}/home\\?season=2025`));
    expect(
      await page.evaluate(() => localStorage.getItem("ibpl_league_select")),
    ).toBe(next === "eurocup" ? "U" : "E");
    expect(
      await page.evaluate(
        () => document.documentElement.scrollWidth <= innerWidth + 1,
      ),
    ).toBe(true);
  });
test("Home card links retain exact scopes and work before data finishes", async ({
  page,
}) => {
  let release!: () => void;
  const held = new Promise<void>((r) => (release = r));
  await routes(page, async (route) => {
    await held;
    return route.fulfill({ json: payload(cases["2026_2"]) });
  });
  await page.goto("/il/home?season=2026&team_id=2");
  await expect(page.locator(".home-nav-card")).toHaveCount(6);
  const links = await page
    .locator(".home-nav-card")
    .evaluateAll((nodes) => nodes.map((n) => n.getAttribute("href")));
  expect(links).toEqual([
    "/il/onoff?season=2026&teams=2",
    "/il/lineups?season=2026&team=2&size=5",
    "/il/team-ratings?season=2026",
    "/il/game-logs?season=2026&team=2",
    "/il/player-stats?season=2026",
    "/il/compare?season=2026&preset=starters_bench",
  ]);
  release();
  await expect(page.locator("#hub_identity .card-title")).toBeVisible();
});
test("Home remembers an opt-in team and restores it on a fresh visit", async ({
  page,
}) => {
  await routes(page);
  await page.goto("/il/home?season=2026&team_id=2");
  await expect(page.locator("#hub_identity .card-title")).toBeVisible();
  await page.getByRole("checkbox", { name: ui.text.defaultLabel }).check();
  expect(await page.evaluate(() => localStorage.getItem("ibplHubTeam"))).toBe(
    "2",
  );
  await pick(
    page,
    "home_team",
    shiny.ilTeams["2026"].find((t) => t.id === 5)!.name,
  );
  await expect(
    page.getByRole("checkbox", { name: ui.text.defaultLabel }),
  ).not.toBeChecked();
  expect(await page.evaluate(() => localStorage.getItem("ibplHubTeam"))).toBe(
    "2",
  );
  await page.goto("/il/home?season=2026");
  await expect(page.locator("#hub_identity .card-title")).toHaveText(
    String(cases["2026_2"].expected.identity!.row.team_name),
  );
  // Shiny restores the team, but the checkbox starts unchecked on a new visit.
  await expect(
    page.getByRole("checkbox", { name: ui.text.defaultLabel }),
  ).not.toBeChecked();
  await page.getByRole("checkbox", { name: ui.text.defaultLabel }).check();
  await page.getByRole("checkbox", { name: ui.text.defaultLabel }).uncheck();
  expect(
    await page.evaluate(() =>
      localStorage.getItem("ibplHubTeamDefaultEnabled"),
    ),
  ).toBeNull();
});
test("Home All teams clears the hub and team-scoped navigation", async ({
  page,
}) => {
  await routes(page);
  await page.goto("/il/home?season=2026&team_id=2");
  await expect(page.locator("#hub_identity .card-title")).toBeVisible();
  await pick(page, "home_team", ui.text.allTeams);
  await expect(page.locator(".home-identity")).toHaveCount(0);
  await expect(page.locator(".home-more-toggle")).toHaveCount(0);
  await expect(
    page.getByRole("checkbox", { name: ui.text.defaultLabel }),
  ).toBeDisabled();
  await expect(page.locator(".home-nav-card").first()).toHaveAttribute(
    "href",
    "/il/onoff?season=2026",
  );
});
test("Home does not replace a new team with a delayed old answer", async ({
  page,
}) => {
  let release!: () => void;
  const held = new Promise<void>((r) => (release = r));
  await routes(page, async (route) => {
    const u = new URL(route.request().url()),
      team = u.searchParams.get("team_id")!;
    if (team === "2") await held;
    return route.fulfill({ json: payload(cases[`2026_${team}`]) });
  });
  await page.goto("/il/home?season=2026&team_id=2");
  await pick(
    page,
    "home_team",
    shiny.ilTeams["2026"].find((t) => t.id === 5)!.name,
  );
  await expect(page.locator("#hub_identity .card-title")).toHaveText(
    String(cases["2026_5"].expected.identity!.row.team_name),
  );
  release();
  await expect(page.locator("#hub_identity .card-title")).toHaveText(
    String(cases["2026_5"].expected.identity!.row.team_name),
  );
});
test("Home reports partial failure, retains healthy sections and retries", async ({
  page,
}) => {
  let failed = true;
  await routes(page, (route) => {
    const data = payload(cases["2026_2"]);
    if (failed) {
      data.frames = { ...data.frames, onoff: [] };
      data.meta.errors = ["onoff"];
    }
    return route.fulfill({ json: data });
  });
  await page.goto("/il/home?season=2026&team_id=2");
  await expect(page.locator('.home-identity [role="alert"]')).toContainText(
    "Some Home sections",
  );
  await expect(page.locator("#hub_identity .card-title")).toBeVisible();
  failed = false;
  await page.getByRole("button", { name: "Retry", exact: true }).click();
  await expect(page.locator('.home-identity [role="alert"]')).toHaveCount(0);
  await openMore(page);
  await expect(page.locator("#hub_players .hub-player-name")).toHaveCount(5);
});
test("Home retries a failed request without losing navigation", async ({
  page,
}) => {
  let failed = true;
  await routes(page, (route) =>
    failed
      ? route.fulfill({ status: 503, json: { error: "Home unavailable" } })
      : route.fulfill({ json: payload(cases["2026_2"]) }),
  );
  await page.goto("/il/home?season=2026&team_id=2");
  await expect(page.locator('.home-identity [role="alert"]')).toContainText(
    "Home unavailable",
  );
  await expect(page.locator(".home-nav-card")).toHaveCount(6);
  failed = false;
  await page.getByRole("button", { name: "Retry", exact: true }).click();
  await expect(page.locator("#hub_identity .card-title")).toBeVisible();
  await expect(page.locator('.home-identity [role="alert"]')).toHaveCount(0);
});
test("Home opens and closes the existing Gameflow surface", async ({
  page,
}, info) => {
  await routes(page);
  await page.goto("/il/home?season=2026&team_id=2");
  await openMore(page);
  await page
    .getByRole("button", { name: "View Gameflow", exact: true })
    .click();
  await expect(
    page.locator("svg.ibpl-ribbon:not(.ibpl-gameflow-gutter)").first(),
  ).toBeVisible();
  if (info.project.name === "desktop")
    await expect(page.getByRole("dialog")).toBeVisible();
  else await expect(page.getByRole("dialog")).toHaveCount(0);
  await page
    .getByRole("button", { name: "Close gameflow", exact: true })
    .click();
  await expect(page.locator("svg.ibpl-ribbon")).toHaveCount(0);
  await expect(
    page.getByRole("button", { name: "View Gameflow", exact: true }),
  ).toBeFocused();
});
test("Home season changes preserve a valid selected team", async ({ page }) => {
  await routes(page);
  await page.goto("/il/home?season=2026&team_id=5");
  await expect(page.locator("#hub_identity .card-title")).toBeVisible();
  if (page.viewportSize()!.width < 1000)
    await page.getByRole("button", { name: "Toggle navigation" }).click();
  await openNavMenu(page, /^Season:/);
  await page
    .locator("#game_year_menu")
    .getByRole("button", { name: "26-27", exact: true })
    .click();
  await expect
    .poll(
      () =>
        new URL(page.url()).searchParams.get("season") ?? shiny.defaultGameYear,
    )
    .toBe("2027");
  await expect(page).toHaveURL(/team_id=5/);
  await expect(page.getByRole("button", { name: /^Season:/ })).toHaveText(
    "Season: 26-27",
  );
  await expect(page.locator(".home-identity")).toHaveAttribute(
    "aria-busy",
    "false",
  );
});

test("[HOME-08][LD-13] Home opens Lineups with its selected team", async ({
  page,
}) => {
  const lineup = loadLineup("il_summary_default.json");
  await routes(page, (route) => {
    const path = new URL(route.request().url()).pathname;
    return route.fulfill({
      json: path.endsWith("/home")
        ? payload(cases["2026_2"])
        : path.endsWith("/lineup-roster")
          ? lineup.roster
          : lineup.answer,
    });
  });
  await page.goto("/il/home?season=2026&team_id=2");
  await page
    .locator(".home-nav-card")
    .filter({
      hasText: ui.cards.il.find((c) => c.input_id === "go_lineups")!.title,
    })
    .click();
  await expect(page).toHaveURL(/\/il\/lineups\?season=2026&team=2&size=5/);
  if (page.viewportSize()!.width < 1000)
    await page
      .getByRole("button", { name: "Show Filters", exact: true })
      .click();
  await expect(page.locator('[data-input-id="ld_team"] .item')).toHaveText(
    shiny.ilTeams["2026"].find((t) => t.id === 2)!.name,
  );
  await expect(page.locator(".lineup-chip").first()).toBeVisible();
  await expect(page.locator("tbody tr").first()).toBeVisible();
});
test("Home See more opens the rest of the hub in place and closes it", async ({
  page,
}) => {
  await routes(page);
  await page.goto("/il/home?season=2026&team_id=2");
  await expect(page.locator("#hub_identity .card-title")).toBeVisible();
  await expect(page.locator(".home-links .home-nav-card")).toHaveCount(6);
  await openMore(page);
  await expect(page.locator("#home-more #hub_storylines")).toBeVisible();
  const box = (s: string) => page.locator(s).boundingBox();
  // Inside the same 640px block, below the two boxes.
  expect((await box("#home-more"))!.y).toBeGreaterThan(
    (await box(".home-boxes"))!.y,
  );
  expect((await box("#home-more"))!.width).toBeLessThanOrEqual(640);
  if (page.viewportSize()!.width >= 576)
    expect((await box("#hub_identity"))!.y).toBe((await box(".home-links"))!.y);
  await page
    .getByRole("button", { name: ui.text.seeLess, exact: true })
    .click();
  await expect(page.locator("#team_hub_section")).toHaveCount(0);
});
