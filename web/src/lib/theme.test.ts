import { describe, expect, it } from "vitest";
import { resolveTheme, THEME_KEY, THEME_SCRIPT } from "./theme";

describe("resolveTheme", () => {
  it("a stored choice wins over the device", () => {
    expect(resolveTheme("light", false)).toBe("light");
    expect(resolveTheme("dark", true)).toBe("dark");
  });
  it("no stored choice follows the device, dark by default", () => {
    expect(resolveTheme(null, true)).toBe("light");
    expect(resolveTheme(null, false)).toBe("dark");
  });
  it("an unknown stored value is ignored", () => {
    expect(resolveTheme("bogus", true)).toBe("light");
    expect(resolveTheme("bogus", false)).toBe("dark");
  });
});

/** Runs the pre-paint script against stubbed browser globals. */
function runScript(stored: string | null | Error, prefersLight: boolean) {
  const el = { dataset: {} as Record<string, string>, style: {} as Record<string, string> };
  const storage = {
    getItem: (k: string) => {
      if (stored instanceof Error) throw stored;
      return k === THEME_KEY ? stored : null;
    },
  };
  const matchMedia = (q: string) => ({
    matches: q === "(prefers-color-scheme: light)" && prefersLight,
  });
  new Function("document", "localStorage", "matchMedia", THEME_SCRIPT)(
    { documentElement: el },
    storage,
    matchMedia,
  );
  return el;
}

describe("THEME_SCRIPT", () => {
  it("applies the same rule as resolveTheme", () => {
    for (const stored of ["light", "dark", null, "bogus"])
      for (const pl of [true, false]) {
        const el = runScript(stored, pl);
        expect(el.dataset.theme).toBe(resolveTheme(stored, pl));
        expect(el.style.colorScheme).toBe(resolveTheme(stored, pl));
      }
  });
  it("storage that throws falls back to the device setting", () => {
    expect(runScript(new Error("blocked"), true).dataset.theme).toBe("light");
    expect(runScript(new Error("blocked"), false).dataset.theme).toBe("dark");
  });
});
