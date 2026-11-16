import { expect, type Page } from "@playwright/test";
/** Opens a navbar text menu, retrying a click that lands before hydration. */
export async function openNavMenu(page: Page, name: RegExp) {
  const button = page.getByRole("button", { name });
  await expect(async () => {
    if ((await button.getAttribute("aria-expanded")) !== "true")
      await button.click();
    await expect(button).toHaveAttribute("aria-expanded", "true", {
      timeout: 1000,
    });
  }).toPass();
}

/** Switches league from the top bar: the switch on wide screens, the menu on
 * narrow ones (Navbar.tsx; web.css shows one). */
export async function chooseLeague(page: Page, name: string) {
  const sw = page.getByRole("group", { name: "League" });
  if (await sw.isVisible()) {
    await sw.getByRole("link", { name, exact: true }).click();
    return;
  }
  await openNavMenu(page, /^League:/);
  await page
    .locator("#league_menu")
    .getByRole("link", { name, exact: true })
    .click();
}
