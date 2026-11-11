import { describe, it, expect } from "vitest";
import { stintRatings } from "./gameflowRatings";
import { ribbonModel, type GameflowEvent, type RawRibbon } from "./gameflow";
import fixture673 from "./__fixtures__/gameflow-673.json";

const ON_TOTALS_673 = [
  { label: "DIBARTOLOMEO", offPoss: 47, offPts: 62, defPoss: 49, defPts: 38 },
  { label: "WALLACE", offPoss: 45, offPts: 68, defPoss: 43, defPts: 41 },
  { label: "BRISSETT", offPoss: 29, offPts: 16, defPoss: 32, defPts: 35 },
];
const FIVE = ["1", "2", "3", "4", "5"];
const ev = (o: Partial<GameflowEvent>): GameflowEvent => ({
  side: "own",
  elapsed: 100,
  order_key: 1,
  off: true,
  players: FIVE,
  pts: 0,
  kind: null,
  made: null,
  poss_end: true,
  ...o,
});

describe("stintRatings", () => {
  it("rates per 100 possessions from counts", () => {
    const events = [
      ev({ pts: 3, order_key: 1 }),
      ev({ pts: 0, order_key: 2 }),
      ev({ off: false, pts: 2, order_key: 3 }),
    ];
    expect(stintRatings(events, "own", ["1"])).toEqual({
      offPoss: 2,
      offPts: 3,
      defPoss: 1,
      defPts: 2,
      ortg: 150,
      drtg: 200,
      net: -50,
    });
  });

  it("counts only rows whose credited five holds every selected player", () => {
    const events = [ev({ players: ["1", "3", "4", "5", "6"], pts: 2 })];
    expect(stintRatings(events, "own", ["1", "2"]).offPoss).toBe(0);
    expect(stintRatings(events, "own", ["1"]).offPoss).toBe(1);
  });

  it("ignores the other side's rows", () => {
    expect(stintRatings([ev({ side: "opp" })], "own", ["1"]).offPoss).toBe(0);
  });

  it("keeps both window ends", () => {
    const w = { start: 50, end: 100 };
    expect(stintRatings([ev({ elapsed: 100 })], "own", ["1"], w).offPoss).toBe(1);
    expect(stintRatings([ev({ elapsed: 50 })], "own", ["1"], w).offPoss).toBe(1);
    expect(stintRatings([ev({ elapsed: 101 })], "own", ["1"], w).offPoss).toBe(0);
  });

  it("adds points scored on a row that does not end a possession", () => {
    const events = [ev({ pts: 1, poss_end: false, kind: "ft", made: true })];
    expect(stintRatings(events, "own", ["1"])).toMatchObject({ offPoss: 0, offPts: 1 });
  });

  it("returns null ratings, never NaN, with no possession", () => {
    expect(stintRatings([], "own", ["1"])).toEqual({
      offPoss: 0,
      offPts: 0,
      defPoss: 0,
      defPts: 0,
      ortg: null,
      drtg: null,
      net: null,
    });
    const onlyOff = stintRatings([ev({ pts: 2 })], "own", ["1"]);
    expect(onlyOff.ortg).toBe(200);
    expect(onlyOff.drtg).toBeNull();
    expect(onlyOff.net).toBeNull();
  });
});

describe("ribbonModel events", () => {
  const model = ribbonModel(fixture673 as unknown as RawRibbon, 2, "il", 673);

  it("maps team ids to sides and player ids to lane keys", () => {
    expect(model.events.length).toBeGreaterThan(0);
    const e = model.events[0];
    expect(["own", "opp"]).toContain(e.side);
    expect(e.players).toHaveLength(5);
    expect(typeof e.players[0]).toBe("string");
  });

  it("is empty when the reader returned none", () => {
    const bare = { ...(fixture673 as unknown as RawRibbon), events: null };
    expect(ribbonModel(bare, 2, "il", 673).events).toEqual([]);
  });

  // ON totals from basketball_test.player_four_factors_by_game, printed by
  // scripts/save_gameflow_fixture.R 673 2 (the same pin as
  // app/tests/testthat/test-gameflow-events-db.R).
  const key = (label: string) =>
    model.lanes.find((l) => l.side === "own" && l.player_label.includes(label))!.player_key;
  it.each(ON_TOTALS_673)("matches On/Off for $label", ({ label, offPoss, offPts, defPoss, defPts }) => {
    expect(stintRatings(model.events, "own", [key(label)])).toMatchObject({
      offPoss,
      offPts,
      defPoss,
      defPts,
    });
  });
});
