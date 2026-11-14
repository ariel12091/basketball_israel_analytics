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

test.describe("the toggle", () => {
  test("switches to light, remembers it and offers dark back", async ({ page }, info) => {
    await page.emulateMedia({ colorScheme: "dark" });
    await page.goto(HOME);
    if (info.project.name === "phone") await page.locator(".navbar-toggle").click();
    const btn = page.locator("#theme_toggle");
    await expect(btn).toHaveAttribute("aria-label", "Switch to light theme");
    await btn.click();
    await expect(page.locator("html")).toHaveAttribute("data-theme", "light");
    expect(await page.evaluate(() => localStorage.getItem("ibpl_theme"))).toBe("light");
    await page.reload();
    await expect(page.locator("html")).toHaveAttribute("data-theme", "light");
    if (info.project.name === "phone") await page.locator(".navbar-toggle").click();
    await expect(btn).toHaveAttribute("aria-label", "Switch to dark theme");
  });

  test("blocked storage still switches for the page", async ({ page }) => {
    await page.addInitScript(() => {
      Storage.prototype.getItem = () => { throw new Error("blocked"); };
      Storage.prototype.setItem = () => { throw new Error("blocked"); };
    });
    await page.emulateMedia({ colorScheme: "light" });
    await page.goto(HOME);
    await expect(page.locator("html")).toHaveAttribute("data-theme", "light");
    await page.evaluate(() => (document.getElementById("theme_toggle") as HTMLButtonElement).click());
    await expect(page.locator("html")).toHaveAttribute("data-theme", "dark");
  });

  test("does not add a navbar row at desktop widths", async ({ page }, info) => {
    test.skip(info.project.name === "phone", "desktop widths only");
    for (const width of [1280, 1366, 1440, 1600]) {
      await page.setViewportSize({ width, height: 900 });
      await page.goto(HOME);
      await expect(page.locator("#last_updated")).not.toBeEmpty();
      const height = () =>
        page.evaluate(() => document.getElementById("main_tabs")!.getBoundingClientRect().height);
      const withToggle = await height();
      await page.addStyleTag({ content: "#theme_toggle{display:none!important}" });
      // Let app.js-style width measurement (--navbar-cluster-w) settle.
      await page.waitForTimeout(300);
      expect(withToggle, `row height at ${width}px`).toBe(await height());
    }
  });
});

/** WCAG contrast ratio of two computed "rgb(r, g, b)" colours. */
function contrast(a: string, b: string) {
  const lum = (c: string) => {
    // color-mix() serialises as "color(srgb 0.8 0.7 0.7)", channels 0-1.
    const unit = c.startsWith("color(") ? 1 : 255;
    const [r, g, bl] = (c.match(/[\d.]+/g) ?? []).slice(0, 3).map(Number).map((v) => {
      const s = v / unit;
      return s <= 0.03928 ? s / 12.92 : ((s + 0.055) / 1.055) ** 2.4;
    });
    return 0.2126 * r + 0.7152 * g + 0.0722 * bl;
  };
  const [x, y] = [lum(a), lum(b)].sort((p, q) => q - p);
  return (x + 0.05) / (y + 0.05);
}

test("light heat cells are tinted and their numbers stay legible", async ({ page }) => {
  const rows = [0, 1].map((rank, i) => ({ game_id: i + 1, team_id: 2, team_name: "Audit team", opp_team_name: "Opponent", game_date: "2026-01-01", gn: i + 1, round_number: i + 1, off_ppp: 100, def_ppp: 100, pr_off_ppp: rank, pr_def_ppp: rank, off_poss: 50, def_poss: 50, has_scores: false }));
  await page.route("**/api/v1/**", (route) => route.fulfill({ json: route.request().url().includes("/version") ? { leagues: [] } : { rows, meta: { version: "audit", view: "summary", teams: [], rounds: [], phases: [], shot_averages: null } } }));
  await page.addInitScript(() => localStorage.setItem("ibpl_theme", "light"));
  await page.goto("/il/game-logs?season=2026");
  const cell = page.locator('tr[data-game-id="1"] td[data-column="off_ppp"]');
  await expect(cell).toBeVisible();
  const { bg, fg, body } = await cell.evaluate((el) => ({
    bg: getComputedStyle(el).backgroundColor,
    fg: getComputedStyle(el).color,
    body: getComputedStyle(document.body).backgroundColor,
  }));
  expect(body).toBe("rgb(250, 248, 244)");
  expect(bg).not.toBe("rgb(110, 38, 34)");
  expect(contrast(bg, "rgb(255, 255, 255)")).toBeLessThan(2); // a light tint
  expect(contrast(bg, fg)).toBeGreaterThanOrEqual(4.5);
});
