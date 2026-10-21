import { test, expect } from "@playwright/test";
test.skip(!process.env.LIVE_DB_SMOKE, "Opt-in read-only live database checks");
for (const [league, season] of [
  ["il", 2026],
  ["euroleague", 2025],
  ["eurocup", 2025],
]) {
  test(`live ${league}: data and phone/desktop interactions`, async ({
    page,
  }, info) => {
    page.on("pageerror", (e) => console.error("PAGE ERROR", e.message));
    page.on("console", (m) => {
      if (m.type() === "error") console.error("CONSOLE", m.text());
    });
    page.on("requestfailed", (r) =>
      console.error("REQUEST FAILED", r.url(), r.failure()?.errorText),
    );
    page.on("response", (r) => {
      if (r.url().includes("/api/"))
        console.log("API RESPONSE", r.status(), r.url());
    });
    await page.goto(`/${league}/game-logs?season=${season}`);
    await expect(page.locator(".dataTables_scrollBody tbody tr").first()).toBeVisible({ timeout: 30000 });
    await expect(page.locator(".dataTables_info")).toContainText("entries");
    if (info.project.name === "phone")
      await page.getByRole("button", { name: "Show Filters", exact: true }).click();
    await page.screenshot({
      path: `../output/playwright/live-${league}-${info.project.name}.png`,
      fullPage: true,
    });
  });
}
