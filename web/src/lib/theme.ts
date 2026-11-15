// Light / dark theme. The theme is an attribute on <html> (data-theme) that
// web/src/app/light.css keys on; dark is Shiny's look and needs no rules.
// Tab pages are CDN-cached, so the server cannot vary the HTML per visitor:
// THEME_SCRIPT runs from <head> before first paint, and the toggle flips the
// attribute in place (no React state, so hydration never mismatches).

export type Theme = "light" | "dark";
export const THEME_KEY = "ibpl_theme";
const LIGHT_QUERY = "(prefers-color-scheme: light)";

/** The visitor's stored choice, else the device setting, else dark. */
export function resolveTheme(
  stored: string | null,
  prefersLight: boolean,
): Theme {
  if (stored === "light" || stored === "dark") return stored;
  return prefersLight ? "light" : "dark";
}

/** resolveTheme, inlined for <head>; theme.test.ts pins the two together. */
export const THEME_SCRIPT = `(function(){var s=null;try{s=localStorage.getItem(${JSON.stringify(THEME_KEY)})}catch(e){}var t=s==="light"||s==="dark"?s:matchMedia(${JSON.stringify(LIGHT_QUERY)}).matches?"light":"dark";var d=document.documentElement;d.dataset.theme=t;d.style.colorScheme=t})();`;

export function currentTheme(): Theme {
  return document.documentElement.dataset.theme === "light" ? "light" : "dark";
}

/** Switches the page and remembers the choice (storage may be blocked).
 * Picking what the device already asks for forgets the override instead,
 * so a later change of the device setting is followed again. */
export function applyTheme(t: Theme): void {
  const d = document.documentElement;
  d.dataset.theme = t;
  d.style.colorScheme = t;
  try {
    if (t === resolveTheme(null, matchMedia(LIGHT_QUERY).matches))
      localStorage.removeItem(THEME_KEY);
    else localStorage.setItem(THEME_KEY, t);
  } catch {
    // Private mode or blocked storage: the switch still holds for this page.
  }
}
