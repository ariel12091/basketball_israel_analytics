"use client";
// The table's tools (CSV, Columns) share the legend's row above the table
// (user 2026-11-16: every icon on one row). The page puts <ToolsSlot /> in
// its .table-top; DataTable renders its toolbar through <InToolsSlot>, which
// portals into that slot when the page has one and stays inline otherwise.
import {
  createContext,
  useContext,
  useMemo,
  useState,
  type ReactNode,
} from "react";
import { createPortal } from "react-dom";

type Slot = { el: HTMLElement | null; set: (el: HTMLElement | null) => void };
const SlotContext = createContext<Slot | null>(null);

export function ToolsSlotProvider({ children }: { children: ReactNode }) {
  const [el, set] = useState<HTMLElement | null>(null);
  const value = useMemo(() => ({ el, set }), [el]);
  return <SlotContext.Provider value={value}>{children}</SlotContext.Provider>;
}

/** Where the table's tools land; extra children (On/Off's own CSV) go first. */
export function ToolsSlot({ children }: { children?: ReactNode }) {
  const slot = useContext(SlotContext);
  return (
    <div className="table-tools" ref={slot?.set}>
      {children}
    </div>
  );
}

export function InToolsSlot({ children }: { children: ReactNode }) {
  const slot = useContext(SlotContext);
  return slot?.el ? createPortal(children, slot.el) : <>{children}</>;
}
