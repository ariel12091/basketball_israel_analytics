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

/** The navbar's light/dark switch. */
export default function ThemeToggle() {
  const theme = useSyncExternalStore(subscribe, currentTheme, serverTheme);
  const next: Theme = theme === "light" ? "dark" : "light";
  const label = `Switch to ${next} theme`;
  return (
    <button
      className="btn btn-sm btn-outline-secondary nav-help-btn nav-theme-btn"
      id="theme_toggle"
      type="button"
      aria-label={label}
      title={label}
      onClick={() => applyTheme(next)}
    >
      <i
        className={`bi ${theme === "light" ? "bi-moon" : "bi-sun"}`}
        aria-hidden="true"
      ></i>
      {/* Shown only in the collapsed (phone) menu, where the button is a
          full-width row; desktop is icon only (navbar width is tight). */}
      <span className="nav-theme-label" aria-hidden="true">
        {next === "light" ? " Light theme" : " Dark theme"}
      </span>
    </button>
  );
}
