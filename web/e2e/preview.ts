import { test as base } from "@playwright/test";

// Under playwright.preview.config.ts the pages come from a protected Vercel
// Preview. The Protection Bypass for Automation secret gets through
// Deployment Protection; it is attached only to requests for that Preview's
// own origin, never to the fonts/CDN the page also loads. Without
// PREVIEW_URL this is the plain Playwright test.
export const test = base.extend({
  context: async ({ context }, use) => {
    const origin = process.env.PREVIEW_URL;
    const secret = process.env.VERCEL_AUTOMATION_BYPASS_SECRET;
    if (origin && secret)
      await context.route(`${origin}/**`, (route) =>
        route.continue({
          headers: {
            ...route.request().headers(),
            "x-vercel-protection-bypass": secret,
          },
        }),
      );
    await use(context);
  },
});
export { expect } from "@playwright/test";
