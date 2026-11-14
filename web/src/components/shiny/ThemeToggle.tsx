"use client";
import { useEffect, useState } from "react";
import { applyTheme, currentTheme, type Theme } from "@/lib/theme";

/**
 * The navbar's light/dark switch. The theme lives on <html data-theme>
 * (set before paint by ThemeScript); this button only reads and flips it.
 * The server and the first client render show the dark-state icon, then an
 * effect syncs to the real attribute, so hydration always matches.
 */
export default function ThemeToggle() {
  const [theme, setTheme] = useState<Theme>("dark");
  useEffect(() => setTheme(currentTheme()), []);
  const next: Theme = theme === "light" ? "dark" : "light";
  const label = `Switch to ${next} theme`;
  return (
    <button
      className="btn btn-sm btn-outline-secondary nav-help-btn nav-theme-btn"
      id="theme_toggle"
      type="button"
      aria-label={label}
      title={label}
      onClick={() => {
        applyTheme(next);
        setTheme(next);
      }}
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
