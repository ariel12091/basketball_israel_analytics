import { test, expect, type Page } from "@playwright/test";

// Light / dark theme (docs/superpowers/specs/2026-11-15-web-light-theme-design.md).
const HOME = "/il/home?team_id=all";

/** The data-theme <html> carried when the DOM was first parsed. */
async function captureFirstTheme(page: Page) {
  await page.addInitScript(() => {
    document.addEventListener("DOMContentLoaded", () => {
      (window as unknown as { __firstTheme?: string }).__firstTheme =
        document.documentElement.dataset.theme;
    });
  });
}
const firstTheme = (page: Page) =>
  page.evaluate(() => (window as unknown as { __firstTheme?: string }).__firstTheme);

test.describe("theme before first paint", () => {
  test("a stored light choice applies before first paint", async ({ page }) => {
    await page.addInitScript(() => localStorage.setItem("ibpl_theme", "light"));
    await captureFirstTheme(page);
    await page.goto(HOME);
    expect(await firstTheme(page)).toBe("light");
  });
  test("no stored choice follows a light device", async ({ page }) => {
    await page.emulateMedia({ colorScheme: "light" });
    await captureFirstTheme(page);
    await page.goto(HOME);
    expect(await firstTheme(page)).toBe("light");
  });
  test("the default is dark", async ({ page }) => {
    await page.emulateMedia({ colorScheme: "dark" });
    await captureFirstTheme(page);
    await page.goto(HOME);
    expect(await firstTheme(page)).toBe("dark");
  });
});
