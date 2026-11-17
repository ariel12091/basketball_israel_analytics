import { expect, it } from "vitest";
import {
  defaultDates,
  needsFilteredPath,
  parseContext,
  seasonBounds,
} from "./contextParams";

// Israeli pages open on 1 October, leaving the pre-season Winner Cup out while
// keeping Winner Cup games from October on (user 2026-11-17). The whole season
// stays a date choice away, and only it may use the season tables (R10).
it("the Israeli default window starts on 1 October and ends with the season", () => {
  expect(defaultDates("il", 2027)).toEqual({
    start: "2026-10-01",
    end: seasonBounds("il", 2027).end,
  });
  expect(seasonBounds("il", 2027).start).toBe("2026-09-01");
});

it("EuroLeague and EuroCup keep their whole season as the default", () => {
  expect(defaultDates("euroleague", 2026)).toEqual(
    seasonBounds("euroleague", 2026),
  );
  expect(defaultDates("eurocup", 2026)).toEqual(seasonBounds("eurocup", 2026));
});

it("an Israeli request with no dates reads the default window, off the fast path", () => {
  const c = parseContext(new URLSearchParams("season=2027"), "il");
  expect([c.start, c.end]).toEqual(["2026-10-01", "2027-07-01"]);
  expect(needsFilteredPath(c)).toBe(true);
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
