import { expect, it } from "vitest";
import { queryDefaults } from "./tabQuery";
import {
  compareDefaults,
  compareSideParams,
  editCompareSide,
  resolveCompareQuery,
  shortSideLabel,
} from "./compareQuery";
import { homeStoryHref } from "./homeQuery";

const defaults = () =>
  queryDefaults("il", { ...compareDefaults, season: "2026" });

it("last N vs season reads side A's last N games and side B's whole season", () => {
  const q = resolveCompareQuery({
    ...defaults(),
    preset: "last_n_season",
    min_gn: "3",
    max_gn: "9",
  });
  const a = compareSideParams(q, "a", "ratings"),
    b = compareSideParams(q, "b", "ratings");
  expect(a.get("last_n")).toBe("10");
  expect(b.has("last_n")).toBe(false);
  expect(a.has("min_gn") || b.has("min_gn")).toBe(false);
  expect(shortSideLabel(q, "a")).toBe("Last 10");
  expect(shortSideLabel(q, "b")).toBe("Season");
  // Only this preset's side carries the window.
  expect(editCompareSide(q, "a", "last_n", "5")).not.toHaveProperty("b_last_n");
  expect(shortSideLabel({ ...q, a_last_n: "5" }, "a")).toBe("Last 5");
});

it("a side's last N replaces the game-number range the readers refuse with it", () => {
  const q = { ...defaults(), a_last_n: "5", min_gn: "3" };
  expect(compareSideParams(q, "a", "ratings").has("min_gn")).toBe(false);
  expect(compareSideParams(q, "b", "ratings").get("min_gn")).toBe("3");
  expect(editCompareSide(q, "a", "last_n", "5")).toMatchObject({
    min_gn: null,
    max_gn: null,
  });
});

it("Players mode does not send a side's last N", () => {
  const q = { ...defaults(), view: "Players", a_last_n: "5" };
  expect(compareSideParams(q, "a", "players").has("last_n")).toBe(false);
});

it("Home's last-10 storyline opens the preset on that team's Detail", () => {
  const href = homeStoryHref("2026", "", "2", "last10");
  expect(href).toBe(
    "/il/compare?season=2026&preset=last_n_season&team_id=2&open_detail=true",
  );
  const params = Object.fromEntries(new URL(href, "http://x").searchParams);
  expect(resolveCompareQuery({ ...defaults(), ...params })).toMatchObject({
    a_last_n: "10",
    a_teams: "2",
    b_teams: "2",
    entity: "2",
    cmp_view: "detail",
  });
});
