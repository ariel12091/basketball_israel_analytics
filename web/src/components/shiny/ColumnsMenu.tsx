"use client";
// The "Columns" menu (user 2026-11-16): every column of the table with a
// switch to show or hide it and arrows to move it, replacing the "Column
// order" box (a dropdown plus Move left / Move right). Dragging a header
// still reorders on desktop; the arrows are the touch and keyboard way.
import * as Popover from "@radix-ui/react-popover";

export type MenuColumn = { id: string; label: string };

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
  onMove: (from: string, to: string) => void;
  onHidden: (ids: string[]) => void;
  onReset: () => void;
}) {
  const shown = columns.filter((c) => !hidden.includes(c.id)).length;
  return (
    <Popover.Root>
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
        >
          <div className="ibpl-columns-head">
            <span className="ibpl-columns-title">Columns</span>
            <button type="button" className="ibpl-columns-reset" onClick={onReset}>
              Reset
            </button>
          </div>
          <ul className="ibpl-columns-list">
            {columns.map((c, i) => {
              const on = !hidden.includes(c.id),
                fixed = locked.includes(c.id);
              return (
                <li key={c.id} className={on ? undefined : "is-hidden"}>
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
