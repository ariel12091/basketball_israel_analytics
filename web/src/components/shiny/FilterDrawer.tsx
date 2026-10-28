"use client";
// The stats tabs' filters panel (design 2026-10-28 § 2, user 2026-10-28):
// opens right under the Filters button at the toolbar's right end, in the
// frontend-v2 drawer's look (uppercase sections, red Clear all). Radix
// Popover anchors it to the button and handles Escape, outside clicks and
// focus. Always mounted (forceMount) so a chip's edit pencil can find and
// focus its control, and each control keeps its local state.
import type { ReactNode } from "react";
import * as Popover from "@radix-ui/react-popover";
import * as Accordion from "@radix-ui/react-accordion";
import text from "@/data/layoutUi.json";

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
  resultLabel,
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
  resultLabel?: string;
}) {
  const t = text.drawer;
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
      >
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
        {resultLabel && (
          <Popover.Close className="filter-drawer-done">
            {resultLabel}
          </Popover.Close>
        )}
      </Popover.Content>
    </Popover.Root>
  );
}
