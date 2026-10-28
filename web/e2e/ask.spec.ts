// The Ask chat panel on every page. Needs a server started with
// ASK_ENABLED=true; /api/v1/ask is answered in the browser (page.route), so no
// model or key is involved. Pages get x-ibpl-client-data so no tab reads the DB.
import { test, expect, type Page } from "@playwright/test";

const answer = (over: Record<string, unknown> = {}) => ({
  type: "final",
  answer: {
    text: "Best: A - B (7.3 net).",
    lang: "en",
    results: [
      {
        id: "r1",
        call: { tool: "lineups", league: "il", query: { team: "2" }, sort: { key: "net_rtg", desc: true } },
        link: "/il/lineups?team=2",
        view: "summary",
        rows: [],
        total: null,
        rowCount: 1,
        version: "v1",
      },
    ],
    evidence: [{ result: "r1", row: "row1", key: "net_rtg", label: "Net RTG", text: "7.3" }],
    links: ["/il/lineups?team=2"],
    fallback: false,
    usage: { inputTokens: 0, outputTokens: 0, costUsd: 0 },
    ...over,
  },
});
const sse = (...events: unknown[]) => events.map((e) => `data: ${JSON.stringify(e)}\n\n`).join("");

async function mockAsk(page: Page, body: string, status = 200, sent: unknown[] = []) {
  await page.route("**/api/v1/ask", async (route) => {
    sent.push(route.request().postDataJSON());
    await route.fulfill({
      status,
      headers: status === 200 ? { "content-type": "text/event-stream" } : { "content-type": "application/json", "retry-after": "1800" },
      body,
    });
  });
}
async function open(page: Page, path = "/il/home?team_id=all") {
  await page.setExtraHTTPHeaders({ "x-ibpl-client-data": "1" });
  await page.goto(path);
  await page.getByRole("button", { name: "Ask" }).click();
  return page.getByRole("dialog", { name: "Ask a question" });
}
async function send(page: Page, q: string) {
  await page.getByLabel("Your question").fill(q);
  await page.getByLabel("Your question").press("Enter");
}

for (const width of [390, 1440]) {
  test.describe(`Ask panel at ${width}px`, () => {
    test.use({ viewport: { width, height: width === 390 ? 844 : 900 } });

    test("ask button opens the panel on every page", async ({ page }) => {
      await page.setExtraHTTPHeaders({ "x-ibpl-client-data": "1" });
      for (const path of [
        "/il/home?team_id=all",
        "/il/onoff",
        "/il/lineups",
        "/il/team-ratings",
        "/il/game-logs",
        "/il/player-stats",
        "/il/compare",
        "/euroleague/home",
      ]) {
        await page.goto(path);
        await page.getByRole("button", { name: "Ask" }).click();
        await expect(page.getByRole("dialog", { name: "Ask a question" }), path).toBeVisible();
        await page.getByRole("button", { name: "Close" }).click();
      }
    });

    test("Home search box starts a new conversation in the panel", async ({ page }) => {
      const sent: { question?: string; history?: unknown[] }[] = [];
      await mockAsk(page, sse(answer()), 200, sent);
      const panel = await open(page);
      await send(page, "an earlier question");
      await expect(panel.getByText("Best: A - B (7.3 net).")).toBeVisible();
      await page.getByRole("button", { name: "Close" }).click();
      await page.getByRole("searchbox", { name: "Ask a question about the stats" }).fill("best lineups of Hapoel");
      await page.getByRole("searchbox", { name: "Ask a question about the stats" }).press("Enter");
      await expect(panel).toBeVisible();
      await expect(panel.getByText("best lineups of Hapoel")).toBeVisible();
      await expect(panel.getByText("an earlier question")).toHaveCount(0);
      expect(sent.at(-1)).toMatchObject({ question: "best lineups of Hapoel", history: [] });
      // The example under the box sends itself.
      await page.getByRole("button", { name: "Close" }).click();
      await page.getByRole("button", { name: "top 5 lineups of Maccabi Tel Aviv", exact: true }).click();
      await expect(panel.getByText("top 5 lineups of Maccabi Tel Aviv")).toBeVisible();
    });

    test("asks and shows answer, evidence and link", async ({ page }) => {
      await mockAsk(page, sse({ type: "status", text: "Reading Lineups…" }, answer()));
      const panel = await open(page);
      await send(page, "top lineups of Maccabi");
      await expect(panel.getByText("Best: A - B (7.3 net).")).toBeVisible();
      await expect(panel.getByRole("columnheader", { name: "Net RTG" })).toBeVisible();
      await expect(panel.getByRole("cell", { name: "7.3" })).toBeVisible();
      await expect(panel.getByRole("link", { name: "Open the page" })).toHaveAttribute("href", "/il/lineups?team=2");
    });

    test("link opens the page with the same filters and the conversation stays", async ({ page }) => {
      await mockAsk(page, sse(answer()));
      const panel = await open(page);
      await send(page, "top lineups of Maccabi");
      await panel.getByRole("link", { name: "Open the page" }).click();
      await expect(page).toHaveURL(/\/il\/lineups\?team=2/);
      await expect(page.getByRole("dialog", { name: "Ask a question" }).getByText("top lineups of Maccabi")).toBeVisible();
    });

    test("conversation survives a tab switch and a reload", async ({ page }) => {
      await mockAsk(page, sse(answer()));
      await open(page);
      await send(page, "top lineups of Maccabi");
      await expect(page.getByText("Best: A - B (7.3 net).")).toBeVisible();
      // A full page load opens with the panel closed; the conversation is kept.
      await page.goto("/il/team-ratings");
      await page.getByRole("button", { name: "Ask" }).click();
      await expect(page.getByText("Best: A - B (7.3 net).")).toBeVisible();
      await page.reload();
      await page.getByRole("button", { name: "Ask" }).click();
      await expect(page.getByText("Best: A - B (7.3 net).")).toBeVisible();
    });

    test("question from a EuroLeague page is sent with league euroleague", async ({ page }) => {
      const sent: { league?: string }[] = [];
      await mockAsk(page, sse(answer()), 200, sent);
      await open(page, "/euroleague/home");
      await send(page, "best offense");
      await expect.poll(() => sent[0]?.league).toBe("euroleague");
    });

    test("Hebrew answer is rtl", async ({ page }) => {
      await mockAsk(page, sse(answer({ text: "החמישייה הטובה: A - B.", lang: "he" })));
      const panel = await open(page);
      await send(page, "מה החמישייה הכי טובה של מכבי");
      await expect(panel.getByText("החמישייה הטובה: A - B.")).toHaveAttribute("dir", "rtl");
    });

    test("fallback shows the page link", async ({ page }) => {
      await mockAsk(page, sse(answer({ text: "Here's the page that answers this.", evidence: [], fallback: true })));
      const panel = await open(page);
      await send(page, "q");
      await expect(panel.getByText("Here's the page that answers this.")).toBeVisible();
      await expect(panel.getByRole("link", { name: "Open the page" })).toBeVisible();
    });

    test("429 shows retry time and keeps the conversation usable", async ({ page }) => {
      await mockAsk(page, JSON.stringify({ error: "Too many", retryAfterSec: 1800 }), 429);
      const panel = await open(page);
      await send(page, "q");
      await expect(panel.getByRole("alert")).toContainText("30 min");
      await expect(page.getByLabel("Your question")).toBeEnabled();
    });

    test("Escape closes and focus returns to the Ask button", async ({ page }) => {
      await open(page);
      await page.keyboard.press("Escape");
      await expect(page.getByRole("dialog", { name: "Ask a question" })).toHaveCount(0);
      await expect(page.getByRole("button", { name: "Ask" })).toBeFocused();
    });

    test("no horizontal overflow", async ({ page }) => {
      await mockAsk(page, sse(answer()));
      await open(page);
      await send(page, "top lineups of Maccabi");
      await expect(page.getByText("Best: A - B (7.3 net).")).toBeVisible();
      const overflow = await page.evaluate(() => document.documentElement.scrollWidth - window.innerWidth);
      expect(overflow).toBeLessThanOrEqual(0);
    });
  });
}
