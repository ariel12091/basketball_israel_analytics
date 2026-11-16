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
  }) => {
    await page.emulateMedia({ colorScheme: "dark" });
    await page.goto(HOME);
    // In the top bar at every width (one copy).
    const btn = page.locator("#theme_toggle");
    await expect(btn).toBeVisible();
    await expect(btn).toHaveAttribute("aria-label", "Switch to light theme");
    // Retry until hydration has attached the click handler.
    await expect(async () => {
      await btn.click();
      await expect(page.locator("html")).toHaveAttribute(
        "data-theme",
        "light",
        { timeout: 2000 },
      );
    }).toPass();
    expect(await page.evaluate(() => localStorage.getItem("ibpl_theme"))).toBe(
      "light",
    );
    await page.reload();
    await expect(page.locator("html")).toHaveAttribute("data-theme", "light");
    await expect(btn).toHaveAttribute("aria-label", "Switch to dark theme");
    // Back to what the device asks for: the override is forgotten.
    await btn.click();
    await expect(page.locator("html")).toHaveAttribute("data-theme", "dark");
    expect(
      await page.evaluate(() => localStorage.getItem("ibpl_theme")),
    ).toBeNull();
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
    // The server-rendered button exists before hydration attaches its click
    // handler, so retry the click until the theme flips.
    await expect(async () => {
      await page.evaluate(() =>
        (document.getElementById("theme_toggle") as HTMLButtonElement).click(),
      );
      await expect(page.locator("html")).toHaveAttribute("data-theme", "dark", {
        timeout: 2000,
      });
    }).toPass();
  });

  test("the top bar stays one row from 1024 px", async ({ browser }, info) => {
    test.skip(info.project.name === "phone", "desktop widths only");
    test.setTimeout(180000);
    // A context WITHOUT the config's x-ibpl-client-data header: sent to the
    // font CDNs too, it fails their CORS check, so no font ever loads and the
    // bar measures narrower than in a real browser.
    const ctx = await browser.newContext({
      baseURL: info.project.use.baseURL,
      colorScheme: "dark",
      extraHTTPHeaders: {},
    });
    const page = await ctx.newPage();
    // Window widths with a 15 px scrollbar: headless Chromium has none.
    for (const width of [1024, 1280, 1366, 1440, 1600]) {
      await page.setViewportSize({ width: width - 15, height: 900 });
      await page.goto("/il/onoff?season=2026");
      await expect(page.locator("#ibpl_updated")).not.toContainText("n/a");
      await expect
        .poll(() =>
          page.evaluate(async () => {
            await document.fonts.ready;
            return document.fonts.check('600 14px "DM Sans"');
          }),
        )
        .toBe(true);
      // One row: the bar keeps its height and nothing in it overflows.
      const fit = await page.evaluate(() => {
        const row = document.querySelector(".ibpl-topbar-row")!;
        return {
          height: row.getBoundingClientRect().height,
          overflow: row.scrollWidth - row.clientWidth,
        };
      });
      expect(fit.height, `bar height at a ${width}px window`).toBeLessThan(57);
      expect(fit.overflow, `bar overflows at a ${width}px window`).toBe(0);
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
  const good = page.locator('tr[data-game-id="1"] td[data-column="def_ppp"]');
  // Any CSS colour (rgb, color(), oklch) as "rgb(r, g, b)": paint it on a
  // canvas and read the pixel back.
  const rgbOf = (sel: string, prop: "backgroundColor" | "color") =>
    page.evaluate(
      ([s, p]) => {
        const c = document.createElement("canvas").getContext("2d")!;
        c.fillStyle = getComputedStyle(document.querySelector(s)!)[
          p as "color"
        ];
        c.fillRect(0, 0, 1, 1);
        const [r, g, b] = c.getImageData(0, 0, 1, 1).data;
        return `rgb(${r}, ${g}, ${b})`;
      },
      [sel, prop],
    );
  const badSel = 'tr[data-game-id="1"] td[data-column="off_ppp"]';
  const goodSel = 'tr[data-game-id="1"] td[data-column="def_ppp"]';
  await expect(good).toBeVisible();
  const body = await page.evaluate(
    () => getComputedStyle(document.body).backgroundColor,
  );
  expect(body).toBe("rgb(250, 248, 244)");
  for (const [sel, hue] of [
    [badSel, "red"],
    [goodSel, "green"],
  ] as const) {
    const bg = await rgbOf(sel, "backgroundColor");
    const fg = await rgbOf(sel, "color");
    // The table's extremes are clearly deep, not a pale pastel (user
    // 2026-11-15, twice: "too dimmed"; the L 0.84 pastel measured 2.0),
    // yet still a light tint under dark text.
    const vsWhite = contrast(bg, "rgb(255, 255, 255)");
    expect(vsWhite, `${hue} cell ${bg}`).toBeGreaterThan(2.2);
    expect(vsWhite, `${hue} cell ${bg}`).toBeLessThan(3.5);
    expect(contrast(bg, fg)).toBeGreaterThanOrEqual(4.5); // legible
    // Clearly coloured, not a washed-out grey (user 2026-11-15: "way too
    // dimmed"): the hue's channel leads the other by a wide margin.
    const [r, g] = (bg.match(/\d+/g) ?? []).map(Number);
    expect(hue === "red" ? r - g : g - r, `${hue} cell ${bg}`).toBeGreaterThan(
      40,
    );
  }
});

test("toggling with a window open flips it in place", async ({ page }) => {
  await page.addInitScript(() => localStorage.setItem("ibpl_theme", "light"));
  await page.goto("/il/onoff?season=2026");
  const box = page.locator(".modal-content").first();
  // The server-rendered button exists before hydration attaches its click
  // handler, so retry the click until the window opens.
  await expect(async () => {
    await page.evaluate(() =>
      (document.getElementById("open_glossary") as HTMLElement).click(),
    );
    await expect(box).toBeVisible({ timeout: 2000 });
  }).toPass();
  await expect(box).toHaveCSS("background-color", "rgb(255, 255, 255)");
  await page.evaluate(() =>
    (document.getElementById("theme_toggle") as HTMLButtonElement).click(),
  );
  await expect(page.locator("html")).toHaveAttribute("data-theme", "dark");
  await expect(box).toHaveCSS("background-color", "rgb(31, 26, 20)");
});
