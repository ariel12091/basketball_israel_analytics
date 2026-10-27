import { defineConfig } from "@playwright/test";
import base from "./playwright.config";

// Runs the live suites against a deployed, protected Preview instead of a
// local server (.github/workflows/web-preview-live.yml runs it for every
// Preview). Needs PREVIEW_URL=https://<deployment>.vercel.app,
// VERCEL_AUTOMATION_BYPASS_SECRET and LIVE_DB_SMOKE=1; e2e/preview.ts sends
// the secret to that origin only.
const url = process.env.PREVIEW_URL;
if (!/^https:\/\/[a-z0-9-]+\.vercel\.app$/.test(url ?? ""))
  throw new Error("PREVIEW_URL must be a https://<deployment>.vercel.app URL");
if (!process.env.VERCEL_AUTOMATION_BYPASS_SECRET)
  throw new Error("VERCEL_AUTOMATION_BYPASS_SECRET is not set");

export default defineConfig({
  ...base,
  webServer: undefined,
  // One page at a time: these read the shared production database.
  workers: 1,
  use: { ...base.use, baseURL: url },
});
