import { expect, it } from "vitest";
import { queryDefaults } from "./tabQuery";
import {
  compareDefaults,
  comparePlayerTeams,
  compareSideParams,
  editCompareSide,
  resolveCompareQuery,
  withCompareSelection,
  comparePresetHref,
} from "./compareQuery";
const defaults = () =>
  queryDefaults("il", { ...compareDefaults, season: "2026" });
it("keeps a direct entity link while resolving a preset", () => {
  const q = resolveCompareQuery({
    ...defaults(),
    preset: "home_away",
    entity: "2",
    cmp_view: "detail",
  });
  expect(q.entity).toBe("2");
  expect(q.a_home_away).toBe("home");
  expect(q.b_home_away).toBe("away");
});
it("restores the Home storyline payload as a pinned team detail", () => {
  const href = comparePresetHref("2026", "starters_bench", "2");
  const params = Object.fromEntries(
    new URL(href, "http://localhost").searchParams,
  );
  const q = resolveCompareQuery({ ...defaults(), ...params });
  expect(q).toMatchObject({
    a_teams: "2",
    b_teams: "2",
    a_own_starters: "gte:3",
    b_own_starters: "lte:2",
    entity: "2",
    view: "Teams",
    cmp_view: "detail",
  });
});
it("date split keeps the shared GN window and uses the following day for B", () => {
  const q = resolveCompareQuery({
    ...defaults(),
    preset: "date_split",
    split_date: "2026-01-01",
    min_gn: "3",
    max_gn: "20",
  });
  const a = compareSideParams(q, "a", "ratings"),
    b = compareSideParams(q, "b", "ratings");
  expect(a.get("end")).toBe("2026-01-01");
  expect(b.get("start")).toBe("2026-01-02");
  expect(a.get("min_gn")).toBe("3");
  expect(b.get("max_gn")).toBe("20");
});
it("GN split keeps shared dates and uses the following game for B", () => {
  const q = resolveCompareQuery({
    ...defaults(),
    preset: "gn_split",
    split_gn: "10",
    start: "2025-11-01",
    end: "2026-02-01",
  });
  const a = compareSideParams(q, "a", "ratings"),
    b = compareSideParams(q, "b", "ratings");
  expect(a.get("max_gn")).toBe("10");
  expect(b.get("min_gn")).toBe("11");
  expect(a.get("start")).toBe("2025-11-01");
  expect(b.get("end")).toBe("2026-02-01");
});
it("mirrors non-defining preset filters without replacing the preset difference", () => {
  const q = resolveCompareQuery({ ...defaults(), preset: "home_away" });
  expect(editCompareSide(q, "a", "outcome", "win")).toMatchObject({
    a_outcome: "win",
    b_outcome: "win",
  });
  expect(editCompareSide(q, "a", "home_away", "away")).not.toHaveProperty(
    "b_home_away",
  );
});
it("does not send an incomplete starter selection to the API", () => {
  expect(
    compareSideParams(
      { ...defaults(), a_own_starters: "gte:" },
      "a",
      "ratings",
    ).has("own_starters"),
  ).toBe(false);
});
it("clears automatic players only for the original default-reset filters", () => {
  const q = { ...defaults(), view: "Players", player_a: "1", player_b: "2" };
  expect(editCompareSide(q, "a", "home_away", "home")).toMatchObject({
    player_a: null,
    player_b: null,
    auto_players: "false",
  });
  expect(editCompareSide(q, "a", "opp_rank_n", "4")).not.toHaveProperty(
    "player_a",
  );
});
it("scopes player reads to the roster, ignoring an invalid optional team", () => {
  const players = [
    { player_id: 1, team_id: 2 },
    { player_id: 1, team_id: 3 },
    { player_id: 2, team_id: 4 },
  ];
  const q = {
    ...defaults(),
    player_a: "1",
    player_b: "2",
    player_team_a: "99",
  };
  expect(comparePlayerTeams(q, "a", players)).toBe("2,3");
  expect(comparePlayerTeams({ ...q, player_team_a: "3" }, "a", players)).toBe(
    "3",
  );
  expect(
    comparePlayerTeams({ ...q, player_compare: "self" }, "b", players),
  ).toBe("2,3");
  expect(comparePlayerTeams({ ...q, player_a: "99" }, "a", players)).toBe("");
});
it("uses server-selected players only for the matching initial automatic selection", () => {
  const q = { ...defaults(), view: "Players" },
    selection = { season: "2026", a: "1", b: "2" };
  expect(withCompareSelection(q, selection).player_a).toBe("1");
  expect(
    withCompareSelection({ ...q, season: "2027" }, selection).player_a,
  ).toBe("");
  expect(
    withCompareSelection({ ...q, auto_players: "false" }, selection).player_a,
  ).toBe("");
  expect(
    withCompareSelection({ ...q, player_a: "3" }, selection).player_a,
  ).toBe("3");
});
