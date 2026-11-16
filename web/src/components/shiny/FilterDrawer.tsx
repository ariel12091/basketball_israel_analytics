"use client";
// The stats tabs' filters panel (design 2026-10-28 § 2): a sheet that
// slides up from the bottom on phones and in from the right on wider
// screens (user 2026-11-16), in the frontend-v2 drawer's look (uppercase
// sections, red Clear all). Radix Popover still owns Escape, outside clicks
// and focus; web.css takes it out of the popper's position. Always mounted
// (forceMount) so a chip's edit pencil can find and focus its control, and
// each control keeps its local state. On phones the sheet's top is a grab
// handle: drag it down to close.
import { useRef, useState, type PointerEvent, type ReactNode } from "react";
import * as Popover from "@radix-ui/react-popover";
import * as Accordion from "@radix-ui/react-accordion";
import text from "@/data/layoutUi.json";
import { PHONE } from "./layoutClasses";

/** How far (px) the sheet must be dragged down to close. */
const DRAG_CLOSE = 80;

export type DrawerSection = {
  id: string;
  title: string;
  body: ReactNode;
};

export default function FilterDrawer({
  trigger,
  open,
  onOpenChange,
  sections,
  openSections,
  onSections,
  footer,
  activeCount,
  resettable = false,
  onClearAll,
}: {
  /** The Filters button the panel opens under. */
  trigger: ReactNode;
  open: boolean;
  onOpenChange: (open: boolean) => void;
  sections: DrawerSection[];
  openSections: string[];
  onSections: (ids: string[]) => void;
  footer?: ReactNode;
  activeCount: number;
  /** Something besides the counted filters can be reset (hand-set minimums). */
  resettable?: boolean;
  onClearAll: () => void;
}) {
  const t = text.drawer;
  const start = useRef<number | null>(null);
  const [drag, setDrag] = useState(0);
  const grip = {
    onPointerDown: (e: PointerEvent<HTMLDivElement>) => {
      if (e.button !== 0 || !window.matchMedia(PHONE).matches) return;
      if ((e.target as HTMLElement).closest("button")) return;
      start.current = e.clientY;
      e.currentTarget.setPointerCapture(e.pointerId);
    },
    onPointerMove: (e: PointerEvent<HTMLDivElement>) => {
      if (start.current !== null)
        setDrag(Math.max(0, e.clientY - start.current));
    },
    onPointerUp: () => {
      if (start.current === null) return;
      start.current = null;
      if (drag > DRAG_CLOSE) onOpenChange(false);
      setDrag(0);
    },
    onPointerCancel: () => {
      start.current = null;
      setDrag(0);
    },
  };
  return (
    <Popover.Root open={open} onOpenChange={onOpenChange}>
      <Popover.Trigger asChild>{trigger}</Popover.Trigger>
      <Popover.Content
        forceMount
        className="filter-drawer"
        aria-labelledby="filter-drawer-title"
        side="bottom"
        align="end"
        sideOffset={8}
        collisionPadding={8}
        // Dropdown menus and date pickers may render outside the panel; a
        // click in them is not a click away from the filters.
        onInteractOutside={(e) => {
          const t = e.target as HTMLElement | null;
          if (t?.closest(".selectize-dropdown, .datepicker, .dropdown-menu"))
            e.preventDefault();
        }}
        // Escape inside a dropdown closes the dropdown, not the panel.
        onEscapeKeyDown={(e) => {
          if ((e.target as HTMLElement | null)?.closest(".selectize-control"))
            e.preventDefault();
        }}
        onOpenAutoFocus={(e) => e.preventDefault()}
        style={drag ? { translate: `0 ${drag}px`, transition: "none" } : undefined}
      >
        {/* Pinned while the filters scroll (user 2026-10-28). */}
        <div className="filter-drawer-top" {...grip}>
          <div className="filter-drawer-grip" aria-hidden="true" />
          <div className="filter-drawer-head">
            <h2 className="filter-drawer-title" id="filter-drawer-title">
              {t.title}
            </h2>
            <Popover.Close className="filter-drawer-close" aria-label={t.close}>
              <span aria-hidden="true">×</span> {t.closeLabel}
            </Popover.Close>
          </div>
          {(activeCount > 0 || resettable) && (
            <button
              type="button"
              className="filter-drawer-clear"
              onClick={onClearAll}
            >
              {t.clearAll}
            </button>
          )}
        </div>
        <Accordion.Root
          type="multiple"
          value={openSections}
          onValueChange={onSections}
          className="filter-drawer-sections"
        >
          {sections.map((s) => (
            <Accordion.Item
              key={s.id}
              value={s.id}
              className="filter-drawer-section"
              data-section={s.id}
            >
              <Accordion.Header asChild>
                <h3 className="filter-drawer-section-head">
                  <Accordion.Trigger className="filter-drawer-section-trigger">
                    {s.title}
                  </Accordion.Trigger>
                </h3>
              </Accordion.Header>
              <Accordion.Content
                forceMount
                className="filter-drawer-section-body"
              >
                {s.body}
              </Accordion.Content>
            </Accordion.Item>
          ))}
        </Accordion.Root>
        {footer && <div className="filter-drawer-footer">{footer}</div>}
        {/* Phones: filters apply as they change; Done just closes the sheet. */}
        <div className="filter-drawer-done-bar">
          <Popover.Close className="filter-drawer-done">{t.done}</Popover.Close>
        </div>
      </Popover.Content>
    </Popover.Root>
  );
}
