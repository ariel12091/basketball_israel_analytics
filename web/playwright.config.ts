import { defineConfig } from "@playwright/test";
export default defineConfig({
  testDir: "./e2e",
  timeout: 90000,
  expect: { timeout: 20000 },
  use: {
    baseURL: "http://localhost:3100",
    // Pages render their first data on the server; the tests substitute
    // stored Shiny answers in the browser, so they ask for client-side data
    // (live-smoke.spec.ts clears it to exercise the server render).
    extraHTTPHeaders: { "x-ibpl-client-data": "1" },
    // A visitor with no stored theme follows the device (lib/theme.ts), and
    // Playwright's default device is light. The specs check Shiny's dark
    // look, so default to a dark device; theme.spec.ts sets its own.
    colorScheme: "dark",
  },
  projects: [
    { name: "phone", use: { viewport: { width: 390, height: 844 } } },
    { name: "desktop", use: { viewport: { width: 1440, height: 900 } } },
  ],
  webServer: {
    command: "npm run dev -- --port 3100",
    // Check readiness without issuing real dashboard reads in offline suites.
    url: "http://localhost:3100/il/home?team_id=all",
    reuseExistingServer: !process.env.CI,
  },
});
