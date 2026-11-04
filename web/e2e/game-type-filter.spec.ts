import { test, expect } from "@playwright/test";
// Winner Cup is one choice holding two codes: it must never show as "10", "34".
test("Winner Cup shows its name in the drawer and the chip", async ({
  page,
}, info) => {
  test.skip(info.project.name === "phone", "the drawer is shared by width");
  await page.goto("/il/game-logs?season=2026&game_type=10,34");
  const chip = page.locator(".filter-chip", { hasText: "Winner Cup" });
  await expect(chip).toHaveCount(1);
  await expect(chip).not.toContainText("Winner Cup, Winner Cup");
  await page.locator(".chips-drawer-toggle").click();
  const control = page
    .getByRole("dialog", { name: "Filters" })
    .locator(".shiny-input-container, .form-group")
    .filter({ hasText: "Game type" })
    .last();
  await expect(control).toContainText("Winner Cup");
  await expect(control).not.toContainText(/\b(10|34)\b/);
});
test("Winner Cup shows its name in Compare's filters", async ({
  page,
}, info) => {
  test.skip(info.project.name === "phone", "the control is shared by width");
  await page.goto("/il/compare?season=2026&a_game_type=10,34");
  const control = page
    .locator(".shiny-input-container, .form-group")
    .filter({ hasText: "Game type" })
    .first();
  await expect(control).toContainText("Winner Cup");
  await expect(control).not.toContainText(/\b(10|34)\b/);
});
