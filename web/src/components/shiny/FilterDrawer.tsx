"use client";
// The stats tabs' filters drawer (design 2026-10-28 § 2), modelled on
// frontend-v2's FilterDrawer: slides in from the right, pushes the page from
// 1200px (web.css), overlays below that, full screen at phone width.
// Non-modal so the pushed table stays usable; Radix gives Escape, focus
// return and labelling. Always mounted (forceMount) so a chip's edit pencil
// can find and focus its control, and each control keeps its local state.
import type { ReactNode } from "react";
import * as Dialog from "@radix-ui/react-dialog";
import * as Accordion from "@radix-ui/react-accordion";
import text from "@/data/layoutUi.json";

export type DrawerSection = {
  id: string;
  title: string;
  body: ReactNode;
};

export default function FilterDrawer({
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
    <Dialog.Root open={open} onOpenChange={onOpenChange} modal={false}>
      <Dialog.Content
        forceMount
        className="filter-drawer"
        aria-describedby={undefined}
        // Pushed layout: the table beside the drawer must stay usable, so an
        // outside click does not close it. Escape and the x still do.
        onInteractOutside={(e) => e.preventDefault()}
        // Escape inside a dropdown closes the dropdown, not the drawer.
        onEscapeKeyDown={(e) => {
          if ((e.target as HTMLElement | null)?.closest(".selectize-control"))
            e.preventDefault();
        }}
        onOpenAutoFocus={(e) => e.preventDefault()}
      >
        <div className="filter-drawer-head">
          <Dialog.Title className="filter-drawer-title">{t.title}</Dialog.Title>
          <Dialog.Close className="filter-drawer-close" aria-label={t.close}>
            ×
          </Dialog.Close>
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
          <Dialog.Close className="filter-drawer-done">
            {resultLabel}
          </Dialog.Close>
        )}
      </Dialog.Content>
    </Dialog.Root>
  );
}
