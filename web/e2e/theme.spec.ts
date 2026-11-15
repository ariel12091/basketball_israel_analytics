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
  page.evaluate(
    () => (window as unknown as { __firstTheme?: string }).__firstTheme,
  );

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
  test("switches to light, remembers it and offers dark back", async ({
    page,
  }, info) => {
    await page.emulateMedia({ colorScheme: "dark" });
    await page.goto(HOME);
    // Phone: in the header bar, outside the burger menu (user 2026-11-15);
    // desktop: in the right cluster. Exactly one copy shows at a width.
    const btn = page.locator(".nav-theme-btn:visible");
    await expect(btn).toHaveCount(1);
    if (info.project.name === "phone")
      await expect(page.locator(".navbar-header .nav-theme-btn")).toBeVisible();
    await expect(btn).toHaveAttribute("aria-label", "Switch to light theme");
    await btn.click();
    await expect(page.locator("html")).toHaveAttribute("data-theme", "light");
    expect(await page.evaluate(() => localStorage.getItem("ibpl_theme"))).toBe(
      "light",
    );
    await page.reload();
    await expect(page.locator("html")).toHaveAttribute("data-theme", "light");
    await expect(btn).toHaveAttribute("aria-label", "Switch to dark theme");
    if (info.project.name === "phone") {
      // The open menu holds no second toggle.
      await page.locator(".navbar-toggle").click();
      await expect(page.locator(".nav-theme-btn:visible")).toHaveCount(1);
    }
  });

  test("blocked storage still switches for the page", async ({ page }) => {
    await page.addInitScript(() => {
      Storage.prototype.getItem = () => {
        throw new Error("blocked");
      };
      Storage.prototype.setItem = () => {
        throw new Error("blocked");
      };
    });
    await page.emulateMedia({ colorScheme: "light" });
    await page.goto(HOME);
    await expect(page.locator("html")).toHaveAttribute("data-theme", "light");
    await page.evaluate(() =>
      (document.getElementById("theme_toggle") as HTMLButtonElement).click(),
    );
    await expect(page.locator("html")).toHaveAttribute("data-theme", "dark");
  });

  test("the tab row stays one line at 1440 and 1600 px", async ({
    browser,
  }, info) => {
    test.skip(info.project.name === "phone", "desktop widths only");
    test.setTimeout(180000);
    // A context WITHOUT the config's x-ibpl-client-data header: sent to the
    // font CDNs too, it fails their CORS check, so no font ever loads and the
    // tabs measure narrower than in a real browser.
    const ctx = await browser.newContext({
      baseURL: info.project.use.baseURL,
      colorScheme: "dark",
      extraHTTPHeaders: {},
    });
    const page = await ctx.newPage();
    // Window widths with a 15 px scrollbar: headless Chromium has none, and
    // a real 1440 px window lost the row to it on the Preview. (1280 and 1366
    // wrapped before the toggle existed: root CLAUDE.md, navbar width.)
    for (const width of [1440, 1600]) {
      await page.setViewportSize({ width: width - 15, height: 900 });
      await page.goto("/il/onoff?season=2026");
      await expect(page.locator("#last_updated")).not.toBeEmpty();
      // Measure with the real fonts: without the icon font every tab is
      // narrower and a wrapped row goes unseen (it did once, on the Preview).
      await expect
        .poll(() =>
          page.evaluate(async () => {
            await document.fonts.ready;
            return (
              document.fonts.check('16px "bootstrap-icons"') &&
              document.fonts.check('600 14px "DM Sans"')
            );
          }),
        )
        .toBe(true);
      // Let Navbar's --navbar-cluster-w padding settle: the row wraps only
      // once the cluster's width is reserved, so an early read passes falsely.
      await page.waitForTimeout(2000);
      // One line: the tab list is no taller than one of its tabs.
      const extra = await page.evaluate(() => {
        const ul = document.getElementById("main_tabs")!;
        const li = ul.querySelector("li")!;
        return (
          ul.getBoundingClientRect().height - li.getBoundingClientRect().height
        );
      });
      expect(extra, `tab row wraps at a ${width}px window`).toBeLessThan(4);
    }
    await ctx.close();
  });
});

/** WCAG contrast ratio of two computed "rgb(r, g, b)" colours. */
function contrast(a: string, b: string) {
  const lum = (c: string) => {
    // color-mix() serialises as "color(srgb 0.8 0.7 0.7)", channels 0-1.
    const unit = c.startsWith("color(") ? 1 : 255;
    const [r, g, bl] = (c.match(/[\d.]+/g) ?? [])
      .slice(0, 3)
      .map(Number)
      .map((v) => {
        const s = v / unit;
        return s <= 0.03928 ? s / 12.92 : ((s + 0.055) / 1.055) ** 2.4;
      });
    return 0.2126 * r + 0.7152 * g + 0.0722 * bl;
  };
  const [x, y] = [lum(a), lum(b)].sort((p, q) => q - p);
  return (x + 0.05) / (y + 0.05);
}

test("light heat cells are tinted and their numbers stay legible", async ({
  page,
}) => {
  const rows = [0, 1].map((rank, i) => ({
    game_id: i + 1,
    team_id: 2,
    team_name: "Audit team",
    opp_team_name: "Opponent",
    game_date: "2026-01-01",
    gn: i + 1,
    round_number: i + 1,
    off_ppp: 100,
    def_ppp: 100,
    pr_off_ppp: rank,
    pr_def_ppp: rank,
    off_poss: 50,
    def_poss: 50,
    has_scores: false,
  }));
  await page.route("**/api/v1/**", (route) =>
    route.fulfill({
      json: route.request().url().includes("/version")
        ? { leagues: [] }
        : {
            rows,
            meta: {
              version: "audit",
              view: "summary",
              teams: [],
              rounds: [],
              phases: [],
              shot_averages: null,
            },
          },
    }),
  );
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

test("toggling with a window open flips it in place", async ({ page }) => {
  await page.addInitScript(() => localStorage.setItem("ibpl_theme", "light"));
  await page.goto("/il/onoff?season=2026");
  await page.evaluate(() =>
    (document.getElementById("open_glossary") as HTMLElement).click(),
  );
  const box = page.locator(".modal-content").first();
  await expect(box).toBeVisible();
  await expect(box).toHaveCSS("background-color", "rgb(255, 255, 255)");
  await page.evaluate(() =>
    (document.getElementById("theme_toggle") as HTMLButtonElement).click(),
  );
  await expect(page.locator("html")).toHaveAttribute("data-theme", "dark");
  await expect(box).toHaveCSS("background-color", "rgb(31, 26, 20)");
});
