import { test, expect } from "@playwright/test";
import { loadTeam, teamPresets } from "../test/teamGoldens";
import { rRound } from "../src/rules/round";
for (const file of teamPresets)
  test(`Team Ratings ${file}`, async ({ page }, info) => {
    const { g, answer, rows } = loadTeam(file),
      errors: string[] = [];
    page.on("pageerror", (e) => errors.push(e.message));
    await page.route("**/api/v1/**", (route) =>
      route.fulfill({
        json: new URL(route.request().url()).pathname.endsWith("/version")
          ? { leagues: [{ league: g.league, version: answer.meta.version }] }
          : answer,
      }),
    );
    await page.goto(
      `/${g.league}/team-ratings?${new URLSearchParams(g.query)}`,
    );
    await expect(page.locator(".dataTables_info")).toHaveText(
      `Showing ${rows.length ? 1 : 0} to ${Math.min(50, rows.length)} of ${rows.length} entries`,
    );
    const trs = await page
      .locator(".ibpl-dt-scroll tbody tr")
      .evaluateAll((trs) =>
        trs.map((tr) =>
          Object.fromEntries(
            [...tr.querySelectorAll("td")].map((td) => [
              td.getAttribute("data-column")!,
              (() => {
                const cell = td.cloneNode(true) as HTMLElement;
                cell.querySelector(".ibpl-row-name")?.remove();
                cell
                  .querySelectorAll("br")
                  .forEach((br) => br.replaceWith("\n"));
                return cell.textContent ?? "";
              })(),
            ]),
          ),
        ),
      );
    for (const tr of trs) {
      const row = rows.find((r) => r.team_name === tr.team_name?.trim());
      expect(row).toBeDefined();
      for (const [key, value] of Object.entries(tr)) {
        const expected = row![key];
        if (key === "team_name") {
          expect(value).toBe(String(expected));
          continue;
        }
        if (key === "game_year") {
          expect(value).toBe(String(expected));
          continue;
        }
        const ranked = `rank_${key}` in row!;
        if (expected == null) {
          expect(value.trim()).toBe(
            ranked ? (g.view === "shot_profile" ? "--" : "NA\n#NA\n—") : "",
          );
          continue;
        }
        const numeric = value
          .split("\n")[0]
          .replaceAll(",", "")
          .replace("%", "");
        expect(Number(numeric), key).toBeCloseTo(
          ranked ? rRound(Number(expected), 1) : Number(expected),
          g.view === "traditional" ||
            ["minutes", "off_pace", "def_pace"].includes(key)
            ? 0
            : 1,
        );
        if (ranked) {
          expect(value).toContain(`#${row![`rank_${key}`]}`);
          if (g.view !== "shot_profile") {
            const d = row![`delta_${key}`];
            expect(value.trim().split("\n").at(-1)).toBe(
              d == null
                ? "—"
                : Number(d) > 0
                  ? `▲${d}`
                  : Number(d) < 0
                    ? `▼${Math.abs(Number(d))}`
                    : "↔",
            );
          }
        }
      }
    }
    expect(
      await page.evaluate(
        () => document.documentElement.scrollWidth <= innerWidth,
      ),
    ).toBe(true);
    expect(errors).toEqual([]);
    if (g.id.endsWith("_default"))
      await page.screenshot({
        path: `../output/playwright/team-${g.league}-${g.view}-${info.project.name}.png`,
        fullPage: true,
      });
  });
test("Traditional column order persists and is usable on phone", async ({
  page,
}) => {
  const { g, answer } = loadTeam("il_traditional_default.json");
  await page.route("**/api/v1/**", (r) =>
    r.fulfill({
      json: new URL(r.request().url()).pathname.endsWith("/version")
        ? { leagues: [] }
        : answer,
    }),
  );
  await page.goto(`/il/team-ratings?${new URLSearchParams(g.query)}`);
  await expect(page.locator(".dataTables_info")).toContainText("14 entries");
  await page.getByText("Column order", { exact: true }).click();
  await page.getByLabel("Reorder column", { exact: true }).selectOption("pts");
  const ids = () =>
    page
      .locator(".ibpl-dt-scroll thead th")
      .evaluateAll((ths) => ths.map((t) => t.getAttribute("data-column")));
  await page.getByRole("button", { name: "Move left", exact: true }).click();
  expect((await ids()).slice(0, 5)).toEqual([
    "team_name",
    "gp",
    "poss_on_floor",
    "pts",
    "minutes",
  ]);
  await page.reload();
  await expect(page.locator(".ibpl-dt-scroll tbody tr").first()).toBeVisible();
  await expect
    .poll(ids)
    .toEqual([
      "team_name",
      "gp",
      "poss_on_floor",
      "pts",
      "minutes",
      ...(await ids()).slice(5),
    ]);
  // The desktop gesture uses the same persisted order as the phone controls.
  if (page.viewportSize()!.width > 768) {
    await page
      .locator('th[data-column="pts"]')
      .dragTo(page.locator('th[data-column="gp"]'));
    await expect
      .poll(async () => (await ids()).slice(0, 5))
      .toEqual(["team_name", "pts", "gp", "poss_on_floor", "minutes"]);
  }
  const download = page.waitForEvent("download");
  await page
    .getByRole("button", { name: "Download Team CSV", exact: true })
    .click();
  expect((await download).suggestedFilename()).toContain(
    "traditional_team_per_game",
  );
});
