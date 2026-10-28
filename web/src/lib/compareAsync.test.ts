import { describe, expect, it } from "vitest";
import { compareDefaultPlayers, comparePairReady } from "./compareAsync";

const completed = (version = "v1") => ({
  data: { meta: { version } },
  isFetching: false,
  isError: false,
});
describe("Compare asynchronous answers", () => {
  it("waits for both sides, regardless of completion order", () => {
    const pending = { isFetching: true, isError: false };
    expect(comparePairReady(completed(), pending)).toBe(false);
    expect(comparePairReady(pending, completed())).toBe(false);
    expect(comparePairReady(completed(), completed())).toBe(true);
  });
  it("does not pair a cached answer while its refresh is pending or failed", () => {
    expect(
      comparePairReady(completed(), { ...completed(), isFetching: true }),
    ).toBe(false);
    expect(
      comparePairReady({ ...completed(), isError: true }, completed()),
    ).toBe(false);
  });
  it("rejects different versions between sides and between detail sections", () => {
    expect(comparePairReady(completed("v1"), completed("v2"))).toBe(false);
    expect(comparePairReady(completed("v2"), completed("v2"), "v1")).toBe(
      false,
    );
    expect(comparePairReady(completed("v2"), completed("v2"), "v2")).toBe(true);
  });
});
describe("Compare default player selection", () => {
  it("ranks by PPG, breaks ties by points, then samples only eligible top-three IDs", () => {
    const rows = [
      { player_id: 1, gp: 10, pts: 100 },
      { player_id: 2, gp: 5, pts: 100 },
      { player_id: 3, gp: 10, pts: 200 },
      { player_id: 4, gp: 10, pts: 300 },
      { player_id: 5, gp: 0, pts: 1000 },
    ];
    expect(compareDefaultPlayers(rows, [1, 2, 3, 4, 5], () => 0.999)).toEqual([
      4, 3,
    ]);
    expect(rows[0].player_id).toBe(1);
    expect(compareDefaultPlayers(rows, [1, 2, 3, 4, 5], () => 0)).toEqual([
      3, 2,
    ]);
  });
  it("falls back to the roster when fewer than two scoring candidates are available", () => {
    expect(
      compareDefaultPlayers(
        [{ player_id: 1, gp: 1, pts: 10 }],
        [2, 3],
        () => 0.999,
      ),
    ).toEqual([2, 3]);
    expect(compareDefaultPlayers([], [], () => 0)).toEqual([]);
  });
});
