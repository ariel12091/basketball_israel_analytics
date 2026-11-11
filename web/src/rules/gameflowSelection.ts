// What the Gameflow chart has selected, hovered and previewed, as one
// reducer: every transition sets all six fields together, so none can be
// left stale by a handler that forgot one.
import { laneKey, type Bar, type Window } from "./gameflow";
import type { Run } from "./gameflowRuns";

/** A bar's stint, or (start 0 to the game's end) a player's whole game. */
export type BarWindow = { bar: Bar; start: number; end: number };

export type Selection = {
  /** Lane keys of the selected players, in the order they were picked. */
  selected: string[];
  /** The stint clicked, or the one selected player's whole game. */
  active: BarWindow | null;
  hover: BarWindow | null;
  hoverBox: DOMRect | null;
  /** The shared lineup clicked in the details (`side:dictIndex`). */
  focus: string | null;
  /** The windows of the lineup row under the pointer. */
  preview: Window[] | null;
  /** The run whose band was clicked: its lineups, both teams. */
  run: Run | null;
};

export type SelectionAction =
  /** A name (no window) or a bar (its window) was clicked. */
  | { type: "toggle"; bar: Bar; window?: BarWindow; bars: Bar[]; end: number }
  | { type: "clear" }
  | { type: "hover"; window: BarWindow | null; box: DOMRect | null }
  /** A shared-lineup row was clicked: focus it, or unfocus it if focused. */
  | { type: "focus"; key: string }
  | { type: "preview"; windows: Window[] | null }
  /** A run's band was clicked: show it, or close it if shown. */
  | { type: "run"; run: Run };

export const EMPTY_SELECTION: Selection = {
  selected: [],
  active: null,
  hover: null,
  hoverBox: null,
  focus: null,
  preview: null,
  run: null,
};

const wholeGame = (w: BarWindow, end: number) => w.start === 0 && w.end === end;

/** The single stint on show: one player selected and a bar, not their whole
 * game, clicked. `end` is the game's last elapsed second. */
export function stintOf(s: Selection, end: number): BarWindow | null {
  return s.selected.length === 1 && s.active && !wholeGame(s.active, end)
    ? s.active
    : null;
}

export function selectionReducer(s: Selection, a: SelectionAction): Selection {
  switch (a.type) {
    case "toggle": {
      const key = laneKey(a.bar),
        cleared = { ...s, focus: null, preview: null, hover: null, run: null },
        stint = stintOf(s, a.end);
      // Another stint of the player on show moves to it, rather than deselecting.
      if (
        a.window &&
        stint &&
        laneKey(stint.bar) === key &&
        stint.start !== a.window.start
      )
        return { ...cleared, active: a.window };
      const selected = s.selected.includes(key)
        ? s.selected.filter((p) => p !== key)
        : [...s.selected, key];
      if (selected.length !== 1) return { ...cleared, selected, active: null };
      const bar =
        selected[0] === key
          ? a.bar
          : a.bars.find((b) => laneKey(b) === selected[0])!;
      // A name selects the player's whole game (every lineup he was part
      // of); a bar selects just that stint (user 2026-10-28).
      return {
        ...cleared,
        selected,
        active:
          selected[0] === key && a.window
            ? a.window
            : { bar, start: 0, end: a.end },
      };
    }
    case "clear":
      return EMPTY_SELECTION;
    case "hover":
      return { ...s, hover: a.window, hoverBox: a.box };
    case "focus":
      return { ...s, focus: s.focus === a.key ? null : a.key, preview: null };
    case "preview":
      return { ...s, preview: a.windows };
    // A run replaces any player selection; its band again closes it.
    case "run":
      return s.run && s.run.start === a.run.start && s.run.side === a.run.side
        ? EMPTY_SELECTION
        : { ...EMPTY_SELECTION, run: a.run };
  }
}
