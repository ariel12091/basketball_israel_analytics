"use client";
import { THEME_SCRIPT } from "@/lib/theme";

/**
 * The pre-paint theme script, first in <head> so a light visitor never sees a
 * dark flash. Same pattern as BodyClassScript: the server sends it as a
 * script; in the browser React only sees it as a data block it never runs.
 */
export default function ThemeScript() {
  return (
    <script
      type={typeof window === "undefined" ? undefined : "text/plain"}
      suppressHydrationWarning
      dangerouslySetInnerHTML={{ __html: THEME_SCRIPT }}
    />
  );
}
