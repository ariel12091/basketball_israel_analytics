import { expect, it } from "vitest";
import {
  defaultDates,
  needsFilteredPath,
  parseContext,
  seasonBounds,
} from "./contextParams";

// Every league opens on its whole season, so a default page answers from the
// season tables (R10). Israeli pages opened on 1 October from 2026-11-17 to
// 2026-11-20, which put every default request on the filtered path; the
// filter chips now name the game types the default includes instead.
it("the Israeli default window is the whole season", () => {
  expect(defaultDates("il", 2027)).toEqual(seasonBounds("il", 2027));
  expect(seasonBounds("il", 2027).start).toBe("2026-09-01");
});

it("EuroLeague and EuroCup keep their whole season as the default", () => {
  expect(defaultDates("euroleague", 2026)).toEqual(
    seasonBounds("euroleague", 2026),
  );
  expect(defaultDates("eurocup", 2026)).toEqual(seasonBounds("eurocup", 2026));
});

it("an Israeli request with no dates reads the whole season, on the fast path", () => {
  const c = parseContext(new URLSearchParams("season=2027"), "il");
  expect([c.start, c.end]).toEqual(["2026-09-01", "2027-07-01"]);
  expect(needsFilteredPath(c)).toBe(false);
});

it("the whole season, asked for explicitly, takes the fast path", () => {
  const c = parseContext(
    new URLSearchParams("season=2027&start=2026-09-01&end=2027-07-01"),
    "il",
  );
  expect(needsFilteredPath(c)).toBe(false);
});

it("a EuroLeague request with no dates stays on the fast path", () => {
  const c = parseContext(new URLSearchParams("season=2026"), "euroleague");
  expect(needsFilteredPath(c)).toBe(false);
});
