"use client";
// The "Columns" menu (user 2026-11-16): every column of the table with a
// switch to show or hide it, a grip to drag it to a new place (mouse and
// touch, on pointer events: the browser's own drag-and-drop does not work on
// phones), and arrows for the keyboard. Dragging a header still reorders on
// desktop too.
import { useRef, useState, type PointerEvent } from "react";
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

  const rows = () =>
    [...(list.current?.children ?? [])] as HTMLElement[];
  // Where the dragged row's centre now sits among the others' centres.
  const overIndex = (d: Drag, centre: number) => {
    const els = rows();
    let over = d.index;
    els.forEach((el, i) => {
      if (i === d.index) return;
      const mid = el.offsetTop + el.offsetHeight / 2;
      // On a row's middle counts as past it: dropping onto a row takes its place.
      if (i < d.index && centre <= mid) over = Math.min(over, i);
      if (i > d.index && centre >= mid) over = Math.max(over, i);
    });
    return over;
  };
  const grip = (i: number) => ({
    onPointerDown: (e: PointerEvent<HTMLSpanElement>) => {
      if (e.button !== 0) return;
      const el = rows()[i];
      if (!el) return;
      e.preventDefault();
      e.currentTarget.setPointerCapture(e.pointerId);
      setDrag({
        index: i,
        startY: e.clientY,
        startScroll: scroller.current?.scrollTop ?? 0,
        centre: el.offsetTop + el.offsetHeight / 2,
        height: el.offsetHeight,
        dy: 0,
        over: i,
      });
    },
    onPointerMove: (e: PointerEvent<HTMLSpanElement>) => {
      if (!drag) return;
      // Near the menu's edges the list scrolls under the finger.
      const box = scroller.current;
      if (box) {
        const r = box.getBoundingClientRect();
        if (e.clientY < r.top + 32) box.scrollTop -= 8;
        else if (e.clientY > r.bottom - 32) box.scrollTop += 8;
      }
      const dy =
        e.clientY - drag.startY + ((box?.scrollTop ?? 0) - drag.startScroll);
      setDrag({ ...drag, dy, over: overIndex(drag, drag.centre + dy) });
    },
    onPointerUp: () => {
      if (!drag) return;
      if (drag.over !== drag.index)
        onMove(columns[drag.index].id, columns[drag.over].id);
      setDrag(null);
    },
    onPointerCancel: () => setDrag(null),
  });
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
    <Popover.Root onOpenChange={(open) => !open && setDrag(null)}>
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
            <span className="ibpl-columns-hint">Drag to reorder</span>
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
                >
                  <span
                    className="ibpl-columns-grip"
                    aria-hidden="true"
                    title="Drag to move"
                    {...grip(i)}
                  >
                    <i className="bi bi-grip-vertical"></i>
                  </span>
                  <label className="ibpl-columns-name">
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
                    <span>{c.label}</span>
                  </label>
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
