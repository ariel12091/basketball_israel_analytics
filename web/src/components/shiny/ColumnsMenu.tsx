"use client";
// The "Columns" menu (user 2026-11-16): every column of the table with a
// switch to show or hide it, drag to reorder, and arrows for the keyboard.
// The whole row is the handle ("make the drag easier"): with a mouse the
// drag starts once the pointer moves a few pixels; by touch after a short
// press-and-hold, so a quick swipe still scrolls the list. Built on pointer
// events on the window (the browser's own drag-and-drop does not work on
// phones, and pointer capture is dropped by some browsers once a row moves).
import { useEffect, useRef, useState, type PointerEvent } from "react";
import * as Popover from "@radix-ui/react-popover";

export type MenuColumn = { id: string; label: string };

type Drag = {
  index: number;
  /** Pointer and list scroll at the start, and the row's centre in the list. */
  startY: number;
  startScroll: number;
  centre: number;
  height: number;
  dy: number;
  over: number;
};

/** Mouse: pixels to move before a press becomes a drag. */
const MOUSE_SLOP = 4;
/** Touch: hold this long (ms) without moving more than TOUCH_SLOP px. */
const HOLD_MS = 220;
const TOUCH_SLOP = 8;
/** Auto-scroll: the edge band (px) and the top speed (px per frame). */
const EDGE = 56;
const MAX_SPEED = 14;

export default function ColumnsMenu({
  columns,
  hidden,
  locked = [],
  onMove,
  onHidden,
  onReset,
}: {
  /** Every column, in the table's current order (hidden ones too). */
  columns: MenuColumn[];
  hidden: string[];
  /** Columns that always show (the row's names). */
  locked?: string[];
  /** Move `from` to the place `to` holds now. */
  onMove: (from: string, to: string) => void;
  onHidden: (ids: string[]) => void;
  onReset: () => void;
}) {
  const shown = columns.filter((c) => !hidden.includes(c.id)).length;
  const list = useRef<HTMLUListElement>(null),
    scroller = useRef<HTMLDivElement>(null);
  const [drag, setDrag] = useState<Drag | null>(null);
  const live = useRef<Drag | null>(null),
    latest = useRef({ columns, onMove });
  latest.current = { columns, onMove };
  const stop = useRef<(() => void) | null>(null);
  useEffect(() => () => stop.current?.(), []);

  const rows = () => [...(list.current?.children ?? [])] as HTMLElement[];
  // Where the dragged row's centre now sits among the others' centres. On a
  // row's middle counts as past it: dropping onto a row takes its place.
  const overIndex = (d: Drag, centre: number) => {
    let over = d.index;
    rows().forEach((el, i) => {
      if (i === d.index) return;
      const mid = el.offsetTop + el.offsetHeight / 2;
      if (i < d.index && centre <= mid) over = Math.min(over, i);
      if (i > d.index && centre >= mid) over = Math.max(over, i);
    });
    return over;
  };

  const press = (i: number) => (e: PointerEvent<HTMLLIElement>) => {
    if (e.button !== 0 || live.current || stop.current) return;
    // The switch and the arrows keep their own clicks.
    if ((e.target as HTMLElement).closest("input, button")) return;
    const el = rows()[i];
    if (!el) return;
    const id = e.pointerId,
      touch = e.pointerType !== "mouse",
      x0 = e.clientX,
      y0 = e.clientY;
    let lastY = y0,
      timer: number | undefined,
      frame: number | undefined;
    if (!touch) e.preventDefault(); // no text selection while dragging
    const begin = () => {
      const d: Drag = {
        index: i,
        startY: y0,
        startScroll: scroller.current?.scrollTop ?? 0,
        centre: el.offsetTop + el.offsetHeight / 2,
        height: el.offsetHeight,
        dy: 0,
        over: i,
      };
      live.current = d;
      update(lastY);
      if (touch) navigator.vibrate?.(10);
      frame = requestAnimationFrame(autoScroll);
    };
    // Held near the top or bottom of the visible list (below the pinned
    // header), the list keeps scrolling, faster nearer the edge, whether or
    // not the finger moves (user 2026-11-16: scrolling up did not work and
    // down was slow).
    const autoScroll = () => {
      const box = scroller.current;
      if (!box || !live.current) return;
      const r = box.getBoundingClientRect(),
        head = box.querySelector(".ibpl-columns-head")?.getBoundingClientRect(),
        top = Math.max(r.top, head?.bottom ?? r.top),
        zone = Math.min(EDGE, (r.bottom - top) / 3);
      let speed = 0;
      if (lastY < top + zone)
        speed = -Math.ceil(MAX_SPEED * Math.min(1, (top + zone - lastY) / zone));
      else if (lastY > r.bottom - zone)
        speed = Math.ceil(
          MAX_SPEED * Math.min(1, (lastY - (r.bottom - zone)) / zone),
        );
      if (speed) {
        const before = box.scrollTop;
        box.scrollTop += speed;
        if (box.scrollTop !== before) update(lastY);
      }
      frame = requestAnimationFrame(autoScroll);
    };
    const update = (y: number) => {
      const d = live.current;
      if (!d) return;
      const box = scroller.current;
      const dy = y - d.startY + ((box?.scrollTop ?? 0) - d.startScroll);
      live.current = { ...d, dy, over: overIndex(d, d.centre + dy) };
      setDrag(live.current);
    };
    const move = (ev: globalThis.PointerEvent) => {
      if (ev.pointerId !== id) return;
      lastY = ev.clientY;
      if (live.current) {
        ev.preventDefault();
        update(ev.clientY);
        return;
      }
      const moved = Math.hypot(ev.clientX - x0, ev.clientY - y0);
      if (!touch && moved >= MOUSE_SLOP) begin();
      // Moving before the hold ends is a scroll, not a drag.
      else if (touch && moved > TOUCH_SLOP) stop.current?.();
    };
    // Once a touch drag has begun, the list must not scroll under it.
    const noScroll = (ev: TouchEvent) => {
      if (live.current) ev.preventDefault();
    };
    const end = (ev: globalThis.PointerEvent) => {
      if (ev.pointerId !== id) return;
      const d = live.current;
      stop.current?.();
      if (ev.type === "pointerup" && d && d.over !== d.index) {
        const { columns: cols, onMove: moveTo } = latest.current;
        moveTo(cols[d.index].id, cols[d.over].id);
      }
    };
    stop.current = () => {
      window.clearTimeout(timer);
      if (frame !== undefined) cancelAnimationFrame(frame);
      window.removeEventListener("pointermove", move);
      window.removeEventListener("pointerup", end);
      window.removeEventListener("pointercancel", end);
      window.removeEventListener("touchmove", noScroll);
      live.current = null;
      stop.current = null;
      setDrag(null);
    };
    window.addEventListener("pointermove", move, { passive: false });
    window.addEventListener("pointerup", end);
    window.addEventListener("pointercancel", end);
    window.addEventListener("touchmove", noScroll, { passive: false });
    if (touch) timer = window.setTimeout(begin, HOLD_MS);
  };

  // While dragging, the rows between the old and new place step aside.
  const shift = (i: number) => {
    if (!drag || i === drag.index) return 0;
    if (drag.over > drag.index && i > drag.index && i <= drag.over)
      return -drag.height;
    if (drag.over < drag.index && i < drag.index && i >= drag.over)
      return drag.height;
    return 0;
  };

  return (
    <Popover.Root onOpenChange={(open) => !open && stop.current?.()}>
      <Popover.Trigger asChild>
        <button type="button" className="btn btn-default btn-sm ibpl-columns-btn">
          <i className="bi bi-layout-three-columns" aria-hidden="true"></i>{" "}
          Columns
          {hidden.length > 0 && (
            <span className="ibpl-columns-count">
              {" "}
              · {shown}/{columns.length}
            </span>
          )}
        </button>
      </Popover.Trigger>
      <Popover.Portal>
        <Popover.Content
          className="ibpl-columns-pop"
          aria-label="Columns"
          side="bottom"
          align="end"
          sideOffset={6}
          collisionPadding={8}
          ref={scroller}
        >
          <div className="ibpl-columns-head">
            <span className="ibpl-columns-title">Columns</span>
            <span className="ibpl-columns-hint">
              <span className="ibpl-columns-hint-pointer">Drag a row to move it</span>
              <span className="ibpl-columns-hint-touch">Hold a row, then drag</span>
            </span>
            <button type="button" className="ibpl-columns-reset" onClick={onReset}>
              Reset
            </button>
          </div>
          <ul
            ref={list}
            className={`ibpl-columns-list${drag ? " is-dragging" : ""}`}
          >
            {columns.map((c, i) => {
              const on = !hidden.includes(c.id),
                fixed = locked.includes(c.id),
                dragged = drag?.index === i;
              return (
                <li
                  key={c.id}
                  className={`${on ? "" : "is-hidden"}${dragged ? " is-dragged" : ""}`}
                  style={{
                    transform: dragged
                      ? `translateY(${drag.dy}px)`
                      : shift(i)
                        ? `translateY(${shift(i)}px)`
                        : undefined,
                  }}
                  onPointerDown={press(i)}
                >
                  <span className="ibpl-columns-grip" aria-hidden="true">
                    <i className="bi bi-grip-vertical"></i>
                  </span>
                  <input
                    type="checkbox"
                    className="ibpl-columns-switch"
                    checked={on}
                    disabled={fixed}
                    aria-label={`Show ${c.label}`}
                    onChange={(e) =>
                      onHidden(
                        e.target.checked
                          ? hidden.filter((h) => h !== c.id)
                          : [...hidden, c.id],
                      )
                    }
                  />
                  <span className="ibpl-columns-name">{c.label}</span>
                  <span className="ibpl-columns-moves">
                    <button
                      type="button"
                      aria-label={`Move ${c.label} earlier`}
                      disabled={i === 0}
                      onClick={() => onMove(c.id, columns[i - 1].id)}
                    >
                      <i className="bi bi-arrow-up" aria-hidden="true"></i>
                    </button>
                    <button
                      type="button"
                      aria-label={`Move ${c.label} later`}
                      disabled={i === columns.length - 1}
                      onClick={() => onMove(c.id, columns[i + 1].id)}
                    >
                      <i className="bi bi-arrow-down" aria-hidden="true"></i>
                    </button>
                  </span>
                </li>
              );
            })}
          </ul>
        </Popover.Content>
      </Popover.Portal>
    </Popover.Root>
  );
}
