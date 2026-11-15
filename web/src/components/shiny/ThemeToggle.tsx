"use client";
import { useSyncExternalStore } from "react";
import { applyTheme, currentTheme, type Theme } from "@/lib/theme";

/** <html data-theme> as an external store: ThemeScript sets it before paint,
 * applyTheme() flips it. The server snapshot is dark, so hydration matches
 * and React then re-renders with the real attribute. */
function subscribe(onChange: () => void) {
  const mo = new MutationObserver(onChange);
  mo.observe(document.documentElement, { attributeFilter: ["data-theme"] });
  return () => mo.disconnect();
}
const serverTheme = (): Theme => "dark";

/** The navbar's light/dark switch, icon only. Navbar renders two copies, the
 * header bar's (collapsed nav) and the right cluster's (desktop); light.css
 * shows one by the body width class, like the "Showing" menus. */
export default function ThemeToggle({ id }: { id: string }) {
  const theme = useSyncExternalStore(subscribe, currentTheme, serverTheme);
  const next: Theme = theme === "light" ? "dark" : "light";
  const label = `Switch to ${next} theme`;
  return (
    <button
      className="btn btn-sm btn-outline-secondary nav-help-btn nav-theme-btn"
      id={id}
      type="button"
      aria-label={label}
      title={label}
      onClick={() => applyTheme(next)}
    >
      <i
        className={`bi ${theme === "light" ? "bi-moon" : "bi-sun"}`}
        aria-hidden="true"
      ></i>
    </button>
  );
}
