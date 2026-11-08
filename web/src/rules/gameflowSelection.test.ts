import { describe, it, expect } from "vitest";
import type { Bar } from "./gameflow";
import {
  EMPTY_SELECTION,
  selectionReducer as reduce,
  stintOf,
  type BarWindow,
  type Selection,
} from "./gameflowSelection";

const END = 2400;
// Only side and player_key matter to the reducer (laneKey).
const bar = (side: "own" | "opp", key: string, start: number, end: number) =>
  ({ side, player_key: key, start_elapsed: start, end_elapsed: end }) as Bar;
const a1 = bar("own", "a", 0, 300),
  a2 = bar("own", "a", 900, 1200),
  b1 = bar("own", "b", 0, 600);
const bars = [a1, a2, b1];
const win = (b: Bar): BarWindow => ({
  bar: b,
  start: b.start_elapsed,
  end: b.end_elapsed,
});
const toggle = (s: Selection, b: Bar, w?: BarWindow) =>
  reduce(s, { type: "toggle", bar: b, window: w, bars, end: END });
const busy: Selection = {
  ...EMPTY_SELECTION,
  hover: win(b1),
  focus: "own:3",
  preview: [{ start: 0, end: 10 }],
};

describe("Gameflow selection reducer", () => {
  it("a name selects the player's whole game; a bar just that stint", () => {
    const name = toggle(EMPTY_SELECTION, a1);
    expect(name.selected).toEqual(["own:a"]);
    expect(name.active).toEqual({ bar: a1, start: 0, end: END });
    expect(stintOf(name, END)).toBeNull();
    const stint = toggle(EMPTY_SELECTION, a1, win(a1));
    expect(stint.active).toEqual(win(a1));
    expect(stintOf(stint, END)).toEqual(win(a1));
  });

  it("another stint of the player on show moves to it, a second click on it deselects", () => {
    const first = toggle(EMPTY_SELECTION, a1, win(a1));
    const moved = toggle(first, a2, win(a2));
    expect(moved.selected).toEqual(["own:a"]);
    expect(moved.active).toEqual(win(a2));
    expect(toggle(moved, a2, win(a2))).toMatchObject({
      selected: [],
      active: null,
    });
  });

  it("two players share no active stint; dropping one gives the other's whole game", () => {
    const two = toggle(toggle(EMPTY_SELECTION, a1), b1);
    expect(two.selected).toEqual(["own:a", "own:b"]);
    expect(two.active).toBeNull();
    const back = toggle(two, a1);
    expect(back.selected).toEqual(["own:b"]);
    expect(back.active).toEqual({ bar: b1, start: 0, end: END });
  });

  it("every toggle drops the hover, the focused lineup and the preview", () => {
    expect(toggle(busy, a1)).toMatchObject({
      hover: null,
      focus: null,
      preview: null,
    });
  });

  it("focus toggles and drops the preview; clear empties everything", () => {
    const on = reduce(busy, { type: "focus", key: "own:7" });
    expect(on).toMatchObject({ focus: "own:7", preview: null });
    expect(reduce(on, { type: "focus", key: "own:7" }).focus).toBeNull();
    expect(reduce(toggle(busy, a1), { type: "clear" })).toEqual(
      EMPTY_SELECTION,
    );
  });
});
