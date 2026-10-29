"use client";
import { BODY_CLASS_SCRIPT } from "./layoutClasses";

/**
 * The pre-paint body-class script (one design per width). The server sends it
 * as a script, so it runs from the HTML before first paint; in the browser
 * React only sees it as a data block, which it neither runs again nor warns
 * about ("Encountered a script tag while rendering React component").
 */
export default function BodyClassScript() {
  return (
    <script
      type={typeof window === "undefined" ? undefined : "text/plain"}
      suppressHydrationWarning
      dangerouslySetInnerHTML={{ __html: BODY_CLASS_SCRIPT }}
    />
  );
}
