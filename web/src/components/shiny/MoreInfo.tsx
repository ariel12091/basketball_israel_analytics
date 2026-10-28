"use client";
// "There's more here" (design 2026-10-28 § 6): a dashed-underlined label that
// opens a popover -- on hover with a mouse, on tap with touch or pen, and
// from the keyboard. Radix Popover handles focus, Escape and outside taps.
import { useRef, useState, type ReactNode } from "react";
import * as Popover from "@radix-ui/react-popover";

export default function MoreInfo({
  label,
  children,
  className,
  ariaLabel,
}: {
  label: ReactNode;
  children: ReactNode;
  className?: string;
  /** Accessible name when `label` is not plain text. */
  ariaLabel?: string;
}) {
  const [open, setOpen] = useState(false);
  const closing = useRef<ReturnType<typeof setTimeout> | null>(null);
  const hover = (next: boolean) => (e: React.PointerEvent) => {
    if (e.pointerType !== "mouse") return;
    if (closing.current) clearTimeout(closing.current);
    if (next) setOpen(true);
    else closing.current = setTimeout(() => setOpen(false), 120);
  };
  return (
    <Popover.Root open={open} onOpenChange={setOpen}>
      <Popover.Trigger
        className={`ibpl-more${className ? ` ${className}` : ""}`}
        aria-label={ariaLabel}
        onPointerEnter={hover(true)}
        onPointerLeave={hover(false)}
        // A tap inside a clickable cell must not also trigger the cell.
        onClick={(e) => e.stopPropagation()}
      >
        {label}
      </Popover.Trigger>
      <Popover.Portal>
        <Popover.Content
          className="ibpl-more-pop"
          sideOffset={6}
          collisionPadding={12}
          onPointerEnter={hover(true)}
          onPointerLeave={hover(false)}
          onOpenAutoFocus={(e) => e.preventDefault()}
        >
          {children}
        </Popover.Content>
      </Popover.Portal>
    </Popover.Root>
  );
}
