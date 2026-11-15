import { describe, it, expect } from "vitest";
import { gameRating, ratingTone, TONE_SPAN } from "./gameflowRatings";
import type { GameflowEvent } from "./gameflow";

describe("ratingTone", () => {
  it("is green for a high ORtg, a low DRtg and a positive Net", () => {
    expect(ratingTone("ortg", 110, 100)).toBeCloseTo(10 / TONE_SPAN);
    expect(ratingTone("drtg", 90, 100)).toBeCloseTo(10 / TONE_SPAN);
    expect(ratingTone("net", 10, 100)).toBeCloseTo(10 / (2 * TONE_SPAN));
  });
  it("is red the other way round, and caps at full", () => {
    expect(ratingTone("ortg", 90, 100)).toBeLessThan(0);
    expect(ratingTone("drtg", 300, 100)).toBe(-1);
    expect(ratingTone("net", 500, null)).toBe(1);
  });
  it("has no tone without a rating or a game average", () => {
    expect(ratingTone("ortg", null, 100)).toBeNull();
    expect(ratingTone("ortg", 100, null)).toBeNull();
  });
});

describe("gameRating", () => {
  const ev = (off: boolean, pts: number): GameflowEvent =>
    ({ side: "own", off, pts, poss_end: true, elapsed: 0, players: [] }) as unknown as GameflowEvent;
  it("is both teams' points over both teams' possessions", () => {
    expect(gameRating([ev(true, 2), ev(true, 0), ev(false, 3), ev(false, 1)])).toBe(150);
  });
  it("is null with no possessions", () => {
    expect(gameRating([])).toBeNull();
  });
});
