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
