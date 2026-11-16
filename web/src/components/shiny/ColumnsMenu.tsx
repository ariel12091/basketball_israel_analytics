"use client";
// The "Columns" menu (user 2026-11-16): a wide panel with every column of
// the table as a chip, in the table's order, wrapping over a few rows so
// they all show at once. Tap a chip to hide or show its column; drag a chip
// to move it (mouse: press and move; touch: press and hold, then drag), an
// amber marker showing where it will land. Alt + arrow keys move the focused
// chip. Built on pointer events on the window: the browser's own
// drag-and-drop does not work on phones, and pointer capture is dropped by
// some browsers once a chip moves.
import { useEffect, useRef, useState, type PointerEvent } from "react";
import * as Popover from "@radix-ui/react-popover";

export type MenuColumn = { id: string; label: string };

type Drag = {
  index: number;
  x0: number;
  y0: number;
  dx: number;
  dy: number;
  /** Insertion slot among the other chips (0 .. n-1): the final index. */
  slot: number;
};

/** Mouse: pixels to move before a press becomes a drag. */
const MOUSE_SLOP = 4;
/** Touch: hold this long (ms) without moving more than TOUCH_SLOP px. */
const HOLD_MS = 220;
const TOUCH_SLOP = 8;
/** Auto-scroll when the panel itself scrolls (small phones). */
const EDGE = 48;
const MAX_SPEED = 12;

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
  const grid = useRef<HTMLDivElement>(null),
    scroller = useRef<HTMLDivElement>(null);
  const [drag, setDrag] = useState<Drag | null>(null);
  const live = useRef<Drag | null>(null),
    latest = useRef({ columns, hidden, locked, onMove, onHidden });
  latest.current = { columns, hidden, locked, onMove, onHidden };
  const stop = useRef<(() => void) | null>(null);
  useEffect(() => () => stop.current?.(), []);

  const toggle = (id: string) => {
    const l = latest.current;
    if (l.locked.includes(id)) return;
    l.onHidden(
      l.hidden.includes(id) ? l.hidden.filter((h) => h !== id) : [...l.hidden, id],
    );
  };
  const moveBy = (i: number, step: number) => {
    const j = i + step;
    if (j < 0 || j >= columns.length) return;
    onMove(columns[i].id, columns[j].id);
  };
  // The slot the pointer is over: before the nearest other chip, or after it
  // when the pointer is past that chip's middle (reading order).
  const slotAt = (index: number, x: number, y: number) => {
    const chips = [...(grid.current?.children ?? [])] as HTMLElement[];
    let best = -1,
      bestDist = Infinity,
      after = false;
    chips.forEach((el, i) => {
      if (i === index) return;
      const r = el.getBoundingClientRect();
      const cx = r.left + r.width / 2,
        cy = r.top + r.height / 2;
      // Rows weigh more than columns: stay on the row the pointer is on.
      const dist = Math.hypot((x - cx) / 2, y - cy);
      if (dist < bestDist) {
        bestDist = dist;
        best = i;
        after = x > cx;
      }
    });
    if (best < 0) return index;
    // Position among the chips without the dragged one.
    const pos = best > index ? best - 1 : best;
    return after ? pos + 1 : pos;
  };

  const press = (i: number) => (e: PointerEvent<HTMLButtonElement>) => {
    if (e.button !== 0 || live.current || stop.current) return;
    const id = e.pointerId,
      touch = e.pointerType !== "mouse",
      x0 = e.clientX,
      y0 = e.clientY;
    let lastX = x0,
      lastY = y0,
      timer: number | undefined,
      frame: number | undefined,
      dragged = false;
    const begin = () => {
      dragged = true;
      live.current = { index: i, x0, y0, dx: 0, dy: 0, slot: i };
      update();
      if (touch) navigator.vibrate?.(10);
      frame = requestAnimationFrame(autoScroll);
    };
    const update = () => {
      const d = live.current;
      if (!d) return;
      live.current = {
        ...d,
        dx: lastX - d.x0,
        dy: lastY - d.y0,
        slot: slotAt(d.index, lastX, lastY),
      };
      setDrag(live.current);
    };
    const autoScroll = () => {
      const box = scroller.current;
      if (!box || !live.current) return;
      if (box.scrollHeight > box.clientHeight) {
        const r = box.getBoundingClientRect(),
          head = box.querySelector(".ibpl-columns-head")?.getBoundingClientRect(),
          top = Math.max(r.top, head?.bottom ?? r.top);
        let speed = 0;
        if (lastY < top + EDGE)
          speed = -Math.ceil(MAX_SPEED * Math.min(1, (top + EDGE - lastY) / EDGE));
        else if (lastY > r.bottom - EDGE)
          speed = Math.ceil(MAX_SPEED * Math.min(1, (lastY - (r.bottom - EDGE)) / EDGE));
        if (speed) {
          box.scrollTop += speed;
          live.current = { ...live.current, y0: live.current.y0 - speed };
          update();
        }
      }
      frame = requestAnimationFrame(autoScroll);
    };
    const move = (ev: globalThis.PointerEvent) => {
      if (ev.pointerId !== id) return;
      lastX = ev.clientX;
      lastY = ev.clientY;
      if (live.current) {
        ev.preventDefault();
        update();
        return;
      }
      const moved = Math.hypot(ev.clientX - x0, ev.clientY - y0);
      if (!touch && moved >= MOUSE_SLOP) begin();
      // Moving before the hold ends is a scroll, not a drag.
      else if (touch && moved > TOUCH_SLOP) {
        dragged = true;
        stop.current?.();
      }
    };
    const noScroll = (ev: TouchEvent) => {
      if (live.current) ev.preventDefault();
    };
    const end = (ev: globalThis.PointerEvent) => {
      if (ev.pointerId !== id) return;
      const d = live.current,
        wasDrag = dragged;
      stop.current?.();
      if (ev.type !== "pointerup") return;
      const { columns: cols, onMove: moveTo } = latest.current;
      if (d && d.slot !== d.index) moveTo(cols[d.index].id, cols[d.slot].id);
      // A press that never became a drag is a tap: hide or show.
      else if (!wasDrag) toggle(cols[i].id);
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

  // The marker: before the chip now at the drop slot, or after the last one.
  const others = drag ? columns.filter((_, i) => i !== drag.index) : [];
  const markBefore = drag && drag.slot < others.length ? others[drag.slot].id : null,
    markAfter =
      drag && drag.slot >= others.length ? others[others.length - 1]?.id : null;

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
              <span className="ibpl-columns-hint-pointer">
                Click to hide or show · drag to move
              </span>
              <span className="ibpl-columns-hint-touch">
                Tap to hide or show · hold, then drag to move
              </span>
            </span>
            <button type="button" className="ibpl-columns-reset" onClick={onReset}>
              Reset
            </button>
          </div>
          <div
            ref={grid}
            className={`ibpl-columns-grid${drag ? " is-dragging" : ""}`}
          >
            {columns.map((c, i) => {
              const on = !hidden.includes(c.id),
                fixed = locked.includes(c.id),
                isDragged = drag?.index === i;
              return (
                <button
                  key={c.id}
                  type="button"
                  className={`ibpl-columns-chip${on ? "" : " is-hidden"}${fixed ? " is-locked" : ""}${isDragged ? " is-dragged" : ""}${markBefore === c.id ? " is-drop-before" : ""}${markAfter === c.id ? " is-drop-after" : ""}`}
                  aria-pressed={on}
                  aria-label={`${c.label}: ${fixed ? "always shown" : on ? "shown" : "hidden"}. Alt + arrow keys move it.`}
                  style={
                    isDragged
                      ? { transform: `translate(${drag.dx}px, ${drag.dy}px)` }
                      : undefined
                  }
                  onPointerDown={press(i)}
                  onClick={(e) => {
                    // Pointer taps are handled on pointerup; this is Enter/Space.
                    if (e.detail === 0) toggle(c.id);
                  }}
                  onKeyDown={(e) => {
                    if (!e.altKey) return;
                    if (e.key === "ArrowLeft" || e.key === "ArrowUp") {
                      e.preventDefault();
                      moveBy(i, -1);
                    } else if (e.key === "ArrowRight" || e.key === "ArrowDown") {
                      e.preventDefault();
                      moveBy(i, 1);
                    }
                  }}
                >
                  <i
                    className={`bi ${fixed ? "bi-lock" : on ? "bi-eye" : "bi-eye-slash"}`}
                    aria-hidden="true"
                  ></i>
                  <span>{c.label}</span>
                </button>
              );
            })}
          </div>
        </Popover.Content>
      </Popover.Portal>
    </Popover.Root>
  );
}
